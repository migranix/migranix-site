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

@app.get("/")
async def root():
    return {"status": "Migranix API v2.0", "timestamp": datetime.utcnow().isoformat()}

@app.post("/api/test-connection")
async def test_connection(req: DBCredentials):
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
        return {"success": True, "message": "Connection successful"}
    except Exception as e:
        raise HTTPException(400, detail=str(e))

@app.post("/api/connect")
async def connect(req: DBCredentials):
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

@app.get("/api/schema")
async def get_schema(session: str):
    if session not in connections:
        raise HTTPException(404, "Session not found")

    conn = connections[session]
    try:
        if "engine" in conn:
            engine = conn["engine"]
            inspector = inspect(engine)
            schema_data = []

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

            for db_name in databases[:5]:
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
            return {"success": True, "schema": [{"name": conn["type"], "tables": []}]}
    except Exception as e:
        raise HTTPException(400, detail=str(e))

# ========== FIXED CORE QUERY RUNTIME BLOCK ==========
@app.post("/api/query")
async def execute_query(req: QueryRequest):
    """Execute SQL query and return results inside thread-safe context pools"""
    if req.session not in connections:
        raise HTTPException(404, "Session not found")

    conn = connections[req.session]
    try:
        if "engine" in conn:
            engine = conn["engine"]
            # Forces context allocation and automatic lifecycle cleanup
            with engine.begin() as c:
                result = c.execute(text(req.query))

                if result.returns_rows:
                    columns = list(result.keys())
                    rows = [dict(zip(columns, row)) for row in result.fetchall()]
                    return {"success": True, "columns": columns, "results": rows}
                else:
                    return {"success": True, "columns": [], "results": [], "message": "Query executed successfully"}
        else:
            raise HTTPException(400, "Query execution not supported for this connection type")
    except Exception as e:
        raise HTTPException(400, detail=str(e))

@app.post("/api/upload-file")
async def upload_file(file: UploadFile = File(...), type: str = Form(...)):
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

        engine = create_engine("sqlite:///:memory:", poolclass=NullPool)
        table_name = file.filename.rsplit('.', 1)[0].replace('-', '_').replace(' ', '_')
        df.to_sql(table_name, engine, index=False)

        session_id = str(uuid.uuid4())
        connections[session_id] = {
            "type": "file",
            "engine": engine,
            "tables": [table_name],
            "table_name": table_name,
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

@app.post("/api/test-cloud")
async def test_cloud(creds: CloudCreds):
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
    try:
        tables = []
        if creds.provider == 's3' and boto3:
            s3 = boto3.client('s3', aws_access_key_id=creds.access_key, aws_secret_access_key=creds.secret_key, region_name=creds.region)
            response = s3.list_objects_v2(Bucket=creds.bucket, Prefix=creds.prefix or '')
            tables = [{"name": obj['Key'], "columns": []} for obj in response.get('Contents', [])[:50]]

        return {"success": True, "tables": tables}
    except Exception as e:
        raise HTTPException(400, detail=str(e))

@app.post("/api/export")
async def export_data(req: ExportRequest):
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

# ========== NEW AI INFRASTRUCTURE (GROQ ROUTING) ==========
async def call_groq_router(prompt: str, system_prompt: str = "") -> str:
    """Execute low-latency completions via Groq LPU Framework"""
    import httpx
    key = GROQ_API_KEY
    if not key:
        raise HTTPException(400, "GROQ_API_KEY environment parameter missing. Configure it in Render console.")
    
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {key}"
            },
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": messages,
                "temperature": 0.1
            }
        )
        data = resp.json()
        if resp.status_code != 200:
            error_msg = data.get("error", {}).get("message", "Groq Infrastructure API Routing error")
            raise HTTPException(400, error_msg)
        
        return data["choices"][0]["message"]["content"].strip()

@app.post("/api/ai-sql")
async def generate_sql(request: dict):
    """Generate SQL from natural language using Groq Pipeline"""
    prompt = request.get("prompt", "")
    schema_context = request.get("schema_context", "")
    
    system = f"""You are an expert compiler. Convert plain English requests directly into clean, valid, executable standard SQL syntax.
{('Active dataset schema metrics: ' + schema_context) if schema_context else ''}
Rules:
- Return ONLY the executable SQL statement string context
- Absolutely NO markdown wrapping symbols, no backticks (```), and no code blocks
- Do not include descriptions, structural explanations, or introductory commentary text"""
    
    sql = await call_groq_router(prompt, system)
    
    sql = sql.strip()
    if sql.startswith("```"): 
        sql = sql.split("\n", 1)[1] if "\n" in sql else sql[3:]
    if sql.endswith("```"): 
        sql = sql[:-3]
    return {"success": True, "sql": sql.strip()}

@app.post("/api/ai-profile")
async def ai_data_profile(request: dict):
    session_id = request.get("session_id", "")
    table_name = request.get("table_name", "")
    
    if session_id not in connections:
        raise HTTPException(400, "No active context sequence")
    
    conn = connections[session_id]
    try:
        if conn["type"] == "file":
            engine = conn["engine"]
            with engine.begin() as c:
                count_result = c.execute(text(f"SELECT COUNT(*) FROM '{table_name}'"))
                total_rows = count_result.fetchone()[0]
                
                cols_result = c.execute(text(f"PRAGMA table_info('{table_name}')"))
                columns = [{"name": r[1], "type": r[2]} for r in cols_result.fetchall()]
                
                sample_result = c.execute(text(f"SELECT * FROM '{table_name}' LIMIT 20"))
                sample_cols = list(sample_result.keys())
                sample_rows = [dict(zip(sample_cols, row)) for row in sample_result.fetchall()]
                
                null_counts = {}
                for col in columns:
                    null_result = c.execute(text(f"SELECT COUNT(*) FROM '{table_name}' WHERE \"{col['name']}\" IS NULL"))
                    null_counts[col["name"]] = null_result.fetchone()[0]
                
                distinct_counts = {}
                for col in columns:
                    try:
                        dist_result = c.execute(text(f"SELECT COUNT(DISTINCT \"{col['name']}\") FROM '{table_name}'"))
                        distinct_counts[col["name"]] = dist_result.fetchone()[0]
                    except:
                        distinct_counts[col["name"]] = -1
        else:
            raise HTTPException(400, "Ecosystem data parsing error.")
        
        col_summary = "\n".join([f"- {c['name']} ({c['type']}): {null_counts.get(c['name'],0)} null values, {distinct_counts.get(c['name'],0)} distinct values" for c in columns])
        sample_str = json.dumps(sample_rows[:5], indent=2, default=str)
        
        prompt = f"""Analyze this structured storage metadata template structure and generate a complete analytical metrics configuration JSON object mapping:

Table Target name: {table_name}
Indices row metrics: {total_rows}
Data attributes summary:
{col_summary}

Top rows array reference elements:
{sample_str}

Respond strictly using this raw structural schema blueprint mapping format without markdown formatting blocks:
{{
  "quality_score": <0-100 configuration int>,
  "summary": "<2-3 descriptive processing statements summary>",
  "column_analysis": [
    {{
      "column": "<attribute_name>",
      "detected_type": "<type>",
      "issues": ["<issue>"],
      "suggestion": "<fix>"
    }}
  ],
  "data_issues": [
    {{
      "severity": "high|medium|low",
      "issue": "<description>",
      "affected_rows": <count>,
      "fix": "<action>"
    }}
  ],
  "cleaning_sql": ["<SQL statement>"]
}}"""
        
        result = await call_groq_router(prompt, "You are a professional dataset parser. Respond ONLY using structural standard JSON text profiles. Omit markdown formatting backticks entirely.")
        result = result.strip()
        if result.startswith("```"): 
            result = result.split("\n", 1)[1]
        if result.endswith("```"): 
            result = result[:-3]
        
        try:
            profile = json.loads(result.strip())
        except:
            profile = {"quality_score": 0, "summary": result, "column_analysis": [], "data_issues": [], "cleaning_sql": []}
        
        profile["total_rows"] = total_rows
        profile["total_columns"] = len(columns)
        profile["columns"] = columns
        profile["null_counts"] = null_counts
        profile["distinct_counts"] = distinct_counts
        
        return {"success": True, "profile": profile}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, str(e))

@app.post("/api/ai-explain")
async def ai_explain_query(request: dict):
    sql = request.get("sql", "")
    if not sql:
        raise HTTPException(400, "No data strings provided")
    
    result = await call_groq_router(
        f"Explain this structured data operations query using brief bullet items targeting non-technical audiences:\n\n{sql}",
        "You are an expert compiler analyst helper."
    )
    return {"success": True, "explanation": result}

@app.post("/api/ai-optimize")
async def ai_optimize_query(request: dict):
    sql = request.get("sql", "")
    schema_context = request.get("schema_context", "")
    if not sql:
        raise HTTPException(400, "No metrics targets specified")
    
    result = await call_groq_router(
        f"Analyze the performance distribution parameters of this statement block and return optimization paths:\n\n{sql}\n\nContext metrics: {schema_context}",
        "You are a professional compiler optimization service."
    )
    return {"success": True, "optimization": result}

@app.post("/api/ai-clean")
async def ai_clean_data(request: dict):
    session_id = request.get("session_id", "")
    table_name = request.get("table_name", "")
    
    if session_id not in connections:
        raise HTTPException(400, "No operational session scope context validated.")
    
    conn = connections[session_id]
    try:
        if conn["type"] != "file":
            raise HTTPException(400, "Handshake validation scope error.")
        
        engine = conn["engine"]
        import pandas as pd_local
        import numpy as np
        
        df = pd_local.read_sql(f"SELECT * FROM '{table_name}'", engine)
        original_rows = len(df)
        original_cols = len(df.columns)
        changes = []
        
        for col in df.select_dtypes(include=['object']).columns:
            before = df[col].copy()
            df[col] = df[col].str.strip()
            trimmed = (before != df[col]).sum()
            if trimmed > 0:
                changes.append(f"Trimmed whitespace metrics inside: {col}")
        
        null_strings = ['NA', 'N/A', 'null', 'NULL', 'None', 'none', '-', '']
        for col in df.select_dtypes(include=['object']).columns:
            mask = df[col].isin(null_strings)
            if mask.sum() > 0:
                df.loc[mask, col] = np.nan
                changes.append(f"Wiped out standard null placeholders context in: {col}")
        
        empty_rows = df.isnull().all(axis=1).sum()
        if empty_rows > 0:
            df = df.dropna(how='all')
            changes.append(f"Wiped out completely sparse null records data rows.")
        
        dupes = df.duplicated().sum()
        if dupes > 0:
            df = df.drop_duplicates()
            changes.append(f"Deduplicated repeating tracking elements structural keys.")
        
        cleaned_table = f"{table_name}_cleaned"
        df.to_sql(cleaned_table, engine, index=False, if_exists='replace')
        
        return {
            "success": True,
            "original_rows": original_rows,
            "cleaned_rows": len(df),
            "removed_rows": original_rows - len(df),
            "original_columns": original_cols,
            "changes": changes,
            "cleaned_table": cleaned_table,
            "message": f"Normalizations applied successfully into table references: {cleaned_table}"
        }
    except Exception as e:
        raise HTTPException(400, str(e))

@app.post("/api/ai-migration-plan")
async def ai_migration_plan(request: dict):
    source_type = request.get("source_type", "")
    target_type = request.get("target_type", "Snowflake")
    schema_info = request.get("schema_info", "")
    row_count = request.get("row_count", "unknown")
    
    prompt = f"""Generate a detailed enterprise target environment routing plan structure template based on parameters:
Source infrastructure elements: {source_type}
Target infrastructure elements: {target_type}
Entity relationship maps structural configuration context: {schema_info}
Scale parameters metrics elements: {row_count}"""
    
    result = await call_groq_router(prompt, "You are a professional systems integration architect.")
    return {"success": True, "plan": result}

# ========== CLEANUP ==========
@app.on_event("shutdown")
async def shutdown():
    for conn in connections.values():
        if "engine" in conn:
            conn["engine"].dispose()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
