"""
Migranix Backend API
FastAPI service for universal database connectivity
Deploy to Render: https://render.com
"""

import os
import json
import uuid
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field

# Database drivers — all optional, checked at connection time
try:
    import psycopg2
except ImportError:
    psycopg2 = None

try:
    import pymysql
except ImportError:
    pymysql = None

try:
    import pyodbc
except ImportError:
    pyodbc = None

import sqlite3

try:
    from sqlalchemy import create_engine, text, inspect, MetaData
    from sqlalchemy.pool import NullPool
except ImportError:
    create_engine = None
    NullPool = None

# Optional cloud drivers
try:
    import snowflake.connector
except ImportError:
    snowflake = None

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None

try:
    import pymongo
except ImportError:
    pymongo = None

try:
    from cassandra.cluster import Cluster
except ImportError:
    Cluster = None

try:
    import boto3
except ImportError:
    boto3 = None

import pandas as pd
import io

# ========== CONFIGURATION ==========
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "migranix-secret-key-32bytes!")

# ========== SESSION STORE ==========
# In production, use Redis. For Render free tier, in-memory dict with cleanup.
connections: Dict[str, Dict[str, Any]] = {}

# ========== PYDANTIC MODELS ==========
class DBCredentials(BaseModel):
    type: str
    credentials: Dict[str, Any]

class QueryRequest(BaseModel):
    session: str
    query: str

class ExportRequest(BaseModel):
    format: str = Field(..., pattern="^(csv|json|excel|parquet)$")
    results: List[Dict[str, Any]]
    query: Optional[str] = None

class CloudCreds(BaseModel):
    provider: str
    bucket: Optional[str] = None
    region: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    prefix: Optional[str] = None
    project: Optional[str] = None
    credentials_json: Optional[str] = None
    account: Optional[str] = None
    container: Optional[str] = None
    sas_token: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    stage: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

# ========== CONNECTION MANAGERS ==========
class ConnectionManager:
    @staticmethod
    def create_postgres(creds: dict) -> str:
        dsn = f"postgresql://{creds['username']}:{creds['password']}@{creds['host']}:{creds.get('port',5432)}/{creds['database']}"
        if creds.get('ssl'):
            dsn += "?sslmode=require"
        engine = create_engine(dsn, poolclass=NullPool)
        return dsn, engine

    @staticmethod
    def create_mysql(creds: dict) -> str:
        dsn = f"mysql+pymysql://{creds['username']}:{creds['password']}@{creds['host']}:{creds.get('port',3306)}/{creds['database']}"
        engine = create_engine(dsn, poolclass=NullPool)
        return dsn, engine

    @staticmethod
    def create_sqlserver(creds: dict) -> str:
        driver = creds.get('driver', 'ODBC Driver 17 for SQL Server')
        dsn = f"mssql+pyodbc://{creds['username']}:{creds['password']}@{creds['host']}:{creds.get('port',1433)}/{creds['database']}?driver={driver.replace(' ', '+')}"
        engine = create_engine(dsn, poolclass=NullPool)
        return dsn, engine

    @staticmethod
    def create_oracle(creds: dict) -> str:
        dsn = f"oracle+cx_oracle://{creds['username']}:{creds['password']}@{creds['host']}:{creds.get('port',1521)}/?service_name={creds['service']}"
        engine = create_engine(dsn, poolclass=NullPool)
        return dsn, engine

    @staticmethod
    def create_db2(creds: dict) -> str:
        dsn = f"db2+ibm_db://{creds['username']}:{creds['password']}@{creds['host']}:{creds.get('port',50000)}/{creds['database']}"
        engine = create_engine(dsn, poolclass=NullPool)
        return dsn, engine

    @staticmethod
    def create_sqlite(creds: dict) -> str:
        dsn = f"sqlite:///{creds['filepath']}"
        engine = create_engine(dsn, poolclass=NullPool)
        return dsn, engine

    @staticmethod
    def create_snowflake(creds: dict) -> str:
        if snowflake is None:
            raise ImportError("snowflake-connector-python not installed")
        conn = snowflake.connector.connect(
            account=creds['account'],
            user=creds['username'],
            password=creds['password'],
            warehouse=creds.get('warehouse'),
            database=creds.get('database'),
            schema=creds.get('schema'),
            role=creds.get('role')
        )
        return "snowflake", conn

    @staticmethod
    def create_bigquery(creds: dict) -> str:
        if bigquery is None:
            raise ImportError("google-cloud-bigquery not installed")
        if creds.get('credentials_json'):
            from google.oauth2 import service_account
            info = json.loads(creds['credentials_json'])
            credentials = service_account.Credentials.from_service_account_info(info)
            client = bigquery.Client(project=creds['project'], credentials=credentials)
        else:
            client = bigquery.Client(project=creds['project'])
        return "bigquery", client

    @staticmethod
    def create_redshift(creds: dict) -> str:
        dsn = f"postgresql+psycopg2://{creds['username']}:{creds['password']}@{creds['host']}:{creds.get('port',5439)}/{creds['database']}"
        engine = create_engine(dsn, poolclass=NullPool)
        return dsn, engine

    @staticmethod
    def create_databricks(creds: dict) -> str:
        from databricks import sql
        conn = sql.connect(
            server_hostname=creds['server_hostname'],
            http_path=creds['http_path'],
            access_token=creds['token']
        )
        return "databricks", conn

    @staticmethod
    def create_mongodb(creds: dict) -> str:
        if pymongo is None:
            raise ImportError("pymongo not installed")
        client = pymongo.MongoClient(creds['uri'])
        db = client[creds.get('database', 'test')]
        return "mongodb", db

    @staticmethod
    def create_cassandra(creds: dict) -> str:
        if Cluster is None:
            raise ImportError("cassandra-driver not installed")
        hosts = [h.strip() for h in creds['hosts'].split(',')]
        cluster = Cluster(hosts, port=creds.get('port', 9042))
        session = cluster.connect(creds.get('keyspace'))
        return "cassandra", session

    @staticmethod
    def create_dynamodb(creds: dict) -> str:
        if boto3 is None:
            raise ImportError("boto3 not installed")
        client = boto3.client(
            'dynamodb',
            region_name=creds['region'],
            aws_access_key_id=creds['access_key'],
            aws_secret_access_key=creds['secret_key']
        )
        return "dynamodb", client

    @staticmethod
    def create_cosmosdb(creds: dict) -> str:
        if pymongo is None:
            raise ImportError("pymongo not installed")
        client = pymongo.MongoClient(creds['uri'], ssl=True)
        db = client[creds.get('database', 'test')]
        return "cosmosdb", db

# ========== FASTAPI APP ==========
app = FastAPI(title="Migranix API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://migranix.in","https://www.migranix.in","http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== HEALTH CHECK ==========
@app.get("/")
async def root():
    return {"status": "Migranix API v2.0", "timestamp": datetime.utcnow().isoformat()}

# ========== CONNECTION ENDPOINTS ==========
@app.post("/api/test-connection")
async def test_connection(req: DBCredentials):
    """Test database connection without saving"""
    try:
        manager = ConnectionManager()
        creator = getattr(manager, f"create_{req.type}", None)
        if not creator:
            raise HTTPException(400, f"Unsupported database type: {req.type}")

        result = creator(req.credentials)
        if isinstance(result, tuple):
            dsn, engine = result
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            engine.dispose()
        else:
            # For non-SQLAlchemy connections
            pass

        return {"success": True, "message": "Connection successful"}
    except Exception as e:
        raise HTTPException(400, detail=str(e))

@app.post("/api/connect")
async def connect(req: DBCredentials):
    """Establish connection and return session ID"""
    try:
        manager = ConnectionManager()
        creator = getattr(manager, f"create_{req.type}", None)
        if not creator:
            raise HTTPException(400, f"Unsupported database type: {req.type}")

        result = creator(req.credentials)
        session_id = str(uuid.uuid4())

        if isinstance(result, tuple):
            dsn, engine = result
            connections[session_id] = {
                "type": req.type,
                "engine": engine,
                "dsn": dsn,
                "created_at": datetime.utcnow().isoformat()
            }
        else:
            conn_type, conn = result
            connections[session_id] = {
                "type": req.type,
                "connection": conn,
                "created_at": datetime.utcnow().isoformat()
            }

        return {"success": True, "session_id": session_id}
    except Exception as e:
        raise HTTPException(400, detail=str(e))

# ========== SCHEMA ENDPOINTS ==========
@app.get("/api/schema")
async def get_schema(session: str):
    """Get database schema (databases -> schemas -> tables -> columns)"""
    if session not in connections:
        raise HTTPException(404, "Session not found")

    conn = connections[session]
    try:
        if "engine" in conn:
            engine = conn["engine"]
            inspector = inspect(engine)
            schema_data = []

            # Get databases
            if conn["type"] in ['postgresql', 'mysql', 'sqlserver', 'redshift']:
                with engine.connect() as c:
                    if conn["type"] == 'postgresql':
                        result = c.execute(text("SELECT datname FROM pg_database WHERE datistemplate = false"))
                        databases = [row[0] for row in result]
                    elif conn["type"] == 'mysql':
                        result = c.execute(text("SHOW DATABASES"))
                        databases = [row[0] for row in result]
                    else:
                        databases = [conn["dsn"].split('/')[-1].split('?')[0]]
            else:
                databases = [conn["dsn"].split('/')[-1].split('?')[0]]

            for db_name in databases[:5]:  # Limit to first 5 databases
                db_info = {"name": db_name, "schemas": []}

                try:
                    schemas = inspector.get_schema_names() or ['public']
                except:
                    schemas = ['public']

                for schema_name in schemas[:10]:
                    try:
                        tables = inspector.get_table_names(schema=schema_name)
                    except:
                        tables = inspector.get_table_names()

                    schema_info = {"name": schema_name, "tables": []}
                    for table_name in tables[:50]:
                        try:
                            columns = inspector.get_columns(table_name, schema=schema_name)
                        except:
                            columns = inspector.get_columns(table_name)

                        table_info = {
                            "name": table_name,
                            "columns": [{"name": col["name"], "type": str(col["type"])} for col in columns[:50]]
                        }
                        schema_info["tables"].append(table_info)

                    db_info["schemas"].append(schema_info)

                schema_data.append(db_info)

            return {"success": True, "schema": schema_data}
        else:
            # Handle non-SQLAlchemy connections
            return {"success": True, "schema": [{"name": conn["type"], "tables": []}]}
    except Exception as e:
        raise HTTPException(400, detail=str(e))

# ========== QUERY ENDPOINTS ==========
@app.post("/api/query")
async def execute_query(req: QueryRequest):
    """Execute SQL query and return results"""
    if req.session not in connections:
        raise HTTPException(404, "Session not found")

    conn = connections[req.session]
    try:
        if "engine" in conn:
            engine = conn["engine"]
            with engine.connect() as c:
                result = c.execute(text(req.query))

                if result.returns_rows:
                    columns = list(result.keys())
                    rows = [dict(zip(columns, row)) for row in result.fetchall()]
                    return {"success": True, "columns": columns, "results": rows}
                else:
                    c.commit()
                    return {"success": True, "columns": [], "results": [], "message": "Query executed successfully"}
        else:
            raise HTTPException(400, "Query execution not supported for this connection type")
    except Exception as e:
        raise HTTPException(400, detail=str(e))

# ========== FILE UPLOAD ENDPOINTS ==========
@app.post("/api/upload-file")
async def upload_file(file: UploadFile = File(...), type: str = Form(...)):
    """Upload and parse file (CSV, Excel, JSON, etc.)"""
    try:
        content = await file.read()

        if type == 'csv':
            df = pd.read_csv(io.BytesIO(content), low_memory=False, encoding_errors='replace')
        elif type == 'excel':
            df = pd.read_excel(io.BytesIO(content), engine='openpyxl')
        elif type == 'json':
            try:
                df = pd.read_json(io.BytesIO(content))
            except:
                raw = json.loads(content)
                if isinstance(raw, list):
                    df = pd.json_normalize(raw, sep='_')
                elif isinstance(raw, dict):
                    for v in raw.values():
                        if isinstance(v, list):
                            df = pd.json_normalize(v, sep='_')
                            break
                    else:
                        df = pd.json_normalize([raw], sep='_')
                else:
                    df = pd.DataFrame([raw])
        elif type == 'parquet':
            df = pd.read_parquet(io.BytesIO(content))
        elif type == 'xml':
            df = pd.read_xml(io.BytesIO(content))
        elif type == 'avro':
            try:
                import fastavro
                reader = fastavro.reader(io.BytesIO(content))
                records = [r for r in reader]
                df = pd.DataFrame(records)
            except ImportError:
                raise HTTPException(400, "Avro support not installed")
        else:
            raise HTTPException(400, f"Unsupported file type: {type}")

        # Create in-memory SQLite for querying
        engine = create_engine("sqlite:///:memory:", poolclass=NullPool)
        table_name = file.filename.rsplit('.', 1)[0].replace('-', '_').replace(' ', '_')
        df.to_sql(table_name, engine, index=False)

        session_id = str(uuid.uuid4())
        connections[session_id] = {
            "type": "sqlite",
            "engine": engine,
            "tables": [table_name],
            "created_at": datetime.utcnow().isoformat()
        }

        columns = [{"name": col, "type": str(dtype)} for col, dtype in df.dtypes.items()]
        return {
            "success": True,
            "session_id": session_id,
            "tables": [{"name": table_name, "columns": columns}]
        }
    except Exception as e:
        raise HTTPException(400, detail=str(e))

# ========== CLOUD STORAGE ENDPOINTS ==========
@app.post("/api/test-cloud")
async def test_cloud(creds: CloudCreds):
    """Test cloud storage connection"""
    try:
        if creds.provider == 's3' and boto3:
            s3 = boto3.client('s3', aws_access_key_id=creds.access_key, aws_secret_access_key=creds.secret_key, region_name=creds.region)
            s3.list_objects_v2(Bucket=creds.bucket, Prefix=creds.prefix or '', MaxKeys=1)
        elif creds.provider == 'gcs':
            from google.cloud import storage
            if creds.credentials_json:
                from google.oauth2 import service_account
                info = json.loads(creds.credentials_json)
                credentials = service_account.Credentials.from_service_account_info(info)
                client = storage.Client(project=creds.project, credentials=credentials)
            else:
                client = storage.Client(project=creds.project)
            bucket = client.bucket(creds.bucket)
            list(bucket.list_blobs(max_results=1))
        elif creds.provider == 'azure':
            from azure.storage.blob import BlobServiceClient
            client = BlobServiceClient(account_url=f"https://{creds.account}.blob.core.windows.net", credential=creds.sas_token)
            list(client.list_containers())
        elif creds.provider == 'snowflake_stage':
            if snowflake is None:
                raise ImportError("snowflake-connector not installed")
            conn = snowflake.connector.connect(
                account=creds.account, user=creds.username, password=creds.password,
                warehouse=creds.warehouse, database=creds.database
            )
            conn.cursor().execute(f"LIST @{creds.stage}")
        else:
            raise HTTPException(400, "Cloud provider not supported or library not installed")

        return {"success": True}
    except Exception as e:
        raise HTTPException(400, detail=str(e))

@app.post("/api/connect-cloud")
async def connect_cloud(creds: CloudCreds):
    """Connect to cloud storage"""
    try:
        tables = []
        if creds.provider == 's3' and boto3:
            s3 = boto3.client('s3', aws_access_key_id=creds.access_key, aws_secret_access_key=creds.secret_key, region_name=creds.region)
            response = s3.list_objects_v2(Bucket=creds.bucket, Prefix=creds.prefix or '')
            tables = [{"name": obj['Key'], "columns": []} for obj in response.get('Contents', [])[:50]]

        return {"success": True, "tables": tables}
    except Exception as e:
        raise HTTPException(400, detail=str(e))

# ========== EXPORT ENDPOINTS ==========
@app.post("/api/export")
async def export_data(req: ExportRequest):
    """Export query results to various formats"""
    try:
        df = pd.DataFrame(req.results)

        if req.format == 'csv':
            output = io.StringIO()
            df.to_csv(output, index=False)
            return StreamingResponse(
                io.BytesIO(output.getvalue().encode()),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename=export.csv"}
            )
        elif req.format == 'json':
            output = io.BytesIO(df.to_json(orient='records').encode())
            return StreamingResponse(output, media_type="application/json")
        elif req.format == 'excel':
            output = io.BytesIO()
            df.to_excel(output, index=False, engine='openpyxl')
            output.seek(0)
            return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        elif req.format == 'parquet':
            output = io.BytesIO()
            df.to_parquet(output, index=False)
            output.seek(0)
            return StreamingResponse(output, media_type="application/octet-stream")
        else:
            raise HTTPException(400, "Unsupported format")
    except Exception as e:
        raise HTTPException(400, detail=str(e))

# ========== AI SQL GENERATION ==========
@app.post("/api/ai-sql")
async def generate_sql(request: dict):
    """Generate SQL from natural language using Groq"""
    if not GROQ_API_KEY:
        raise HTTPException(400, "GROQ_API_KEY not configured")

    import httpx
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": "You are an expert SQL assistant. Convert natural language to SQL."},
                    {"role": "user", "content": request.get("prompt", "")}
                ],
                "temperature": 0.1,
                "max_tokens": 1024
            }
        )
        return response.json()

# ========== CLEANUP ==========
@app.on_event("shutdown")
async def shutdown():
    for conn in connections.values():
        if "engine" in conn:
            conn["engine"].dispose()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
