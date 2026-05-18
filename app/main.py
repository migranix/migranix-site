"""
Migranix Data Platform — FastAPI Backend
Handles: DB connections, query execution, AI SQL generation, exports, credential encryption

OPTIONAL DRIVERS (install separately if needed):
  pip install pyodbc oracledb snowflake-connector-python google-cloud-bigquery 
  pip install redshift-connector databricks-sql-connector cassandra-driver ibm-db

Core drivers included: psycopg2, pymysql, pymongo
"""


import os
import json
import base64
import asyncio
import hashlib
import tempfile
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from contextlib import asynccontextmanager
from io import StringIO, BytesIO

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
import httpx

# Database drivers
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import pymysql
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    import pyodbc
    MSSQL_AVAILABLE = True
except ImportError:
    MSSQL_AVAILABLE = False

try:
    import oracledb
    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False

try:
    import ibm_db
    DB2_AVAILABLE = True
except ImportError:
    DB2_AVAILABLE = False

try:
    import snowflake.connector
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

try:
    import pymongo
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

try:
    from google.cloud import bigquery
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False

try:
    import redshift_connector
    REDSHIFT_AVAILABLE = True
except ImportError:
    REDSHIFT_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False

# Supabase REST API client (lightweight, no heavy dependencies)
import asyncio

class SupabaseClient:
    def __init__(self, url, key):
        self.url = url.rstrip('/')
        self.key = key
        self.headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }

    def _sync_request(self, method, path, json_data=None, params=None):
        import httpx
        url = f"{self.url}/rest/v1/{path}"
        kwargs = {'headers': self.headers}
        if json_data:
            kwargs['json'] = json_data
        if params:
            kwargs['params'] = params
        resp = httpx.request(method, url, **kwargs)
        if resp.status_code >= 400:
            raise Exception(f"Supabase error {resp.status_code}: {resp.text}")
        return resp.json() if resp.text else []

    def table(self, name):
        return TableQuery(self, name)

class TableQuery:
    def __init__(self, client, table_name):
        self.client = client
        self.table_name = table_name
        self._select = '*'
        self._filters = {}
        self._order = None
        self._single = False

    def select(self, cols):
        self._select = cols
        return self

    def eq(self, col, val):
        self._filters[col] = f'eq.{val}'
        return self

    def order(self, col, desc=False):
        self._order = f'{col}.{"desc" if desc else "asc"}'
        return self

    def single(self):
        self._single = True
        return self

    def insert(self, data):
        result = self.client._sync_request('POST', self.table_name, json_data=data)
        return {'data': result if isinstance(result, list) else [result]}

    def update(self, data):
        result = self.client._sync_request('PATCH', self.table_name, json_data=data, params=self._filters)
        return {'data': result if isinstance(result, list) else [result]}

    def delete(self):
        result = self.client._sync_request('DELETE', self.table_name, params=self._filters)
        return {'data': result if isinstance(result, list) else [result]}

    def execute(self):
        params = dict(self._filters)
        params['select'] = self._select
        if self._order:
            params['order'] = self._order
        if self._single:
            self.client.headers['Accept'] = 'application/vnd.pgrst.object+json'
        result = self.client._sync_request('GET', self.table_name, params=params)
        if self._single:
            self.client.headers.pop('Accept', None)
        return {'data': result if isinstance(result, list) else [result]}

# Load env
# Encryption setup
_fernet = None
if CRYPTO_AVAILABLE and ENCRYPTION_KEY:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=b'migranix_salt_v1',
        iterations=480000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(ENCRYPTION_KEY.encode()))
    _fernet = Fernet(key)

def encrypt_text(plain: str) -> str:
    if not _fernet:
        return base64.b64encode(plain.encode()).decode()
    return _fernet.encrypt(plain.encode()).decode()

def decrypt_text(cipher: str) -> str:
    if not _fernet:
        return base64.b64decode(cipher.encode()).decode()
    return _fernet.decrypt(cipher.encode()).decode()

# ========== PYDANTIC MODELS ==========

class ConnectionCredentials(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    ssl: bool = False
    # Extra fields for specific DBs
    account: Optional[str] = None  # Snowflake
    warehouse: Optional[str] = None  # Snowflake
    project_id: Optional[str] = None  # BigQuery
    region: Optional[str] = None  # BigQuery, DynamoDB
    cluster: Optional[str] = None  # Redshift, Databricks
    endpoint: Optional[str] = None  # CosmosDB, DynamoDB
    connection_string: Optional[str] = None  # Oracle, DB2, generic
    api_key: Optional[str] = None  # Cloud services
    service_account_json: Optional[str] = None  # BigQuery
    # File/cloud storage
    bucket: Optional[str] = None
    path: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None

class SaveConnectionRequest(BaseModel):
    user_id: str
    name: str
    db_type: str
    credentials: ConnectionCredentials

class TestConnectionRequest(BaseModel):
    db_type: str
    credentials: ConnectionCredentials

class ExecuteQueryRequest(BaseModel):
    connection_id: str
    user_id: str
    query: str
    page: int = 1
    page_size: int = 100

class AISQLRequest(BaseModel):
    natural_language: str
    db_type: str
    schema_hint: Optional[str] = ""

class ExportRequest(BaseModel):
    connection_id: str
    user_id: str
    query: str
    format: str  # csv, excel, json, parquet

# ========== ACTIVE CONNECTIONS POOL ==========
active_connections: Dict[str, Any] = {}

# ========== DB CONNECTOR FUNCTIONS ==========

def connect_postgres(creds: ConnectionCredentials):
    if not POSTGRES_AVAILABLE:
        raise HTTPException(500, "PostgreSQL driver not installed. Run: pip install psycopg2-binary")
    sslmode = "require" if creds.ssl else "prefer"
    conn = psycopg2.connect(
        host=creds.host,
        port=creds.port or 5432,
        database=creds.database,
        user=creds.username,
        password=creds.password,
        sslmode=sslmode,
        connect_timeout=10
    )
    return conn

def connect_mysql(creds: ConnectionCredentials):
    if not MYSQL_AVAILABLE:
        raise HTTPException(500, "MySQL driver not installed. Run: pip install pymysql")
    conn = pymysql.connect(
        host=creds.host,
        port=creds.port or 3306,
        database=creds.database,
        user=creds.username,
        password=creds.password,
        ssl={'ssl': {'ssl-mode': 'REQUIRED'}} if creds.ssl else None,
        connect_timeout=10,
        cursorclass=pymysql.cursors.DictCursor
    )
    return conn

def connect_mssql(creds: ConnectionCredentials):
    if not MSSQL_AVAILABLE:
        raise HTTPException(500, "SQL Server driver not installed. Run: pip install pyodbc")
    driver = "ODBC Driver 18 for SQL Server"
    conn_str = (
        f"DRIVER={{{driver}}};SERVER={creds.host},{creds.port or 1433};"
        f"DATABASE={creds.database};UID={creds.username};PWD={creds.password};"
        f"TrustServerCertificate=yes;"
    )
    if creds.ssl:
        conn_str += "Encrypt=yes;"
    conn = pyodbc.connect(conn_str, timeout=10)
    return conn

def connect_oracle(creds: ConnectionCredentials):
    if not ORACLE_AVAILABLE:
        raise HTTPException(500, "Oracle driver not installed. Run: pip install oracledb")
    if creds.connection_string:
        conn = oracledb.connect(creds.connection_string)
    else:
        dsn = oracledb.makedsn(creds.host, creds.port or 1521, service_name=creds.database)
        conn = oracledb.connect(user=creds.username, password=creds.password, dsn=dsn)
    return conn

def connect_db2(creds: ConnectionCredentials):
    if not DB2_AVAILABLE:
        raise HTTPException(500, "DB2 driver not installed")
    if creds.connection_string:
        conn = ibm_db.connect(creds.connection_string, "", "")
    else:
        conn_str = (
            f"DATABASE={creds.database};HOSTNAME={creds.host};PORT={creds.port or 50000};"
            f"PROTOCOL=TCPIP;UID={creds.username};PWD={creds.password};"
        )
        conn = ibm_db.connect(conn_str, "", "")
    return conn

def connect_snowflake(creds: ConnectionCredentials):
    if not SNOWFLAKE_AVAILABLE:
        raise HTTPException(500, "Snowflake driver not installed. Run: pip install snowflake-connector-python")
    conn = snowflake.connector.connect(
        account=creds.account,
        user=creds.username,
        password=creds.password,
        database=creds.database,
        warehouse=creds.warehouse,
        role="ACCOUNTADMIN",
        login_timeout=10
    )
    return conn

def connect_mongodb(creds: ConnectionCredentials):
    if not MONGODB_AVAILABLE:
        raise HTTPException(500, "MongoDB driver not installed. Run: pip install pymongo")
    if creds.connection_string:
        client = pymongo.MongoClient(creds.connection_string, serverSelectionTimeoutMS=10000)
    else:
        uri = f"mongodb://{creds.username}:{creds.password}@{creds.host}:{creds.port or 27017}/{creds.database}"
        if creds.ssl:
            uri += "?ssl=true"
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=10000)
    client.admin.command('ping')
    return client

def connect_bigquery(creds: ConnectionCredentials):
    if not BIGQUERY_AVAILABLE:
        raise HTTPException(500, "BigQuery driver not installed. Run: pip install google-cloud-bigquery")
    if creds.service_account_json:
        from google.oauth2 import service_account
        info = json.loads(creds.service_account_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        client = bigquery.Client(project=creds.project_id, credentials=credentials)
    else:
        client = bigquery.Client(project=creds.project_id)
    return client

def connect_redshift(creds: ConnectionCredentials):
    if not REDSHIFT_AVAILABLE:
        raise HTTPException(500, "Redshift driver not installed. Run: pip install redshift-connector")
    conn = redshift_connector.connect(
        host=creds.host,
        port=creds.port or 5439,
        database=creds.database,
        user=creds.username,
        password=creds.password,
        ssl=creds.ssl
    )
    return conn

def connect_databricks(creds: ConnectionCredentials):
    # Databricks uses similar connection to Spark
    from databricks import sql
    conn = sql.connect(
        server_hostname=creds.host,
        http_path=creds.endpoint or "/sql/1.0/endpoints/...",
        access_token=creds.api_key or creds.password
    )
    return conn

def connect_mariadb(creds: ConnectionCredentials):
    # MariaDB uses same driver as MySQL
    return connect_mysql(creds)

def connect_cassandra(creds: ConnectionCredentials):
    from cassandra.cluster import Cluster
    cluster = Cluster([creds.host], port=creds.port or 9042)
    session = cluster.connect(creds.database)
    return session

# Connector router
CONNECTORS = {
    "postgresql": connect_postgres,
    "mysql": connect_mysql,
    "mariadb": connect_mariadb,
    "mssql": connect_mssql,
    "oracle": connect_oracle,
    "db2": connect_db2,
    "snowflake": connect_snowflake,
    "mongodb": connect_mongodb,
    "bigquery": connect_bigquery,
    "redshift": connect_redshift,
    "databricks": connect_databricks,
    "cassandra": connect_cassandra,
}

# ========== SCHEMA EXTRACTION ==========

def get_postgres_schema(conn):
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name, ordinal_position
    """)
    rows = cursor.fetchall()
    cursor.close()
    return build_schema_tree(rows, "table_schema", "table_name", "column_name", "data_type")

def get_mysql_schema(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
        ORDER BY table_schema, table_name, ordinal_position
    """)
    rows = cursor.fetchall()
    cursor.close()
    return build_schema_tree(rows, "table_schema", "table_name", "column_name", "data_type")

def get_mssql_schema(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """)
    rows = cursor.fetchall()
    cursor.close()
    return build_schema_tree(rows, 0, 1, 2, 3)

def get_oracle_schema(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT owner, table_name, column_name, data_type
        FROM all_tab_columns
        WHERE owner NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'MDSYS')
        ORDER BY owner, table_name, column_id
    """)
    rows = cursor.fetchall()
    cursor.close()
    return build_schema_tree(rows, 0, 1, 2, 3)

def get_snowflake_schema(conn):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema != 'INFORMATION_SCHEMA'
        ORDER BY table_schema, table_name, ordinal_position
    """)
    rows = cursor.fetchall()
    cursor.close()
    return build_schema_tree(rows, 0, 1, 2, 3)

def get_mongodb_schema(client, db_name):
    db = client[db_name]
    schema = {"databases": []}
    for dbn in client.list_database_names():
        if dbn in ["admin", "local", "config"]:
            continue
        db_obj = {"name": dbn, "schemas": [{"name": "collections", "tables": []}]}
        for coll_name in client[dbn].list_collection_names():
            sample = client[dbn][coll_name].find_one()
            cols = []
            if sample:
                for k, v in sample.items():
                    cols.append({"name": k, "type": type(v).__name__})
            db_obj["schemas"][0]["tables"].append({"name": coll_name, "columns": cols})
        schema["databases"].append(db_obj)
    return schema

def get_bigquery_schema(client):
    schema = {"databases": []}
    for dataset in client.list_datasets():
        ds_obj = {"name": dataset.dataset_id, "schemas": [{"name": "tables", "tables": []}]}
        for table in client.list_tables(dataset.dataset_id):
            tref = client.get_table(table.reference)
            cols = [{"name": f.name, "type": f.field_type} for f in tref.schema]
            ds_obj["schemas"][0]["tables"].append({"name": table.table_id, "columns": cols})
        schema["databases"].append(ds_obj)
    return schema

def build_schema_tree(rows, schema_key, table_key, col_key, type_key):
    schema = {"databases": []}
    db_map = {}
    for row in rows:
        if isinstance(row, dict):
            s = row[schema_key]
            t = row[table_key]
            c = row[col_key]
            dt = row[type_key]
        else:
            s, t, c, dt = row[0], row[1], row[2], row[3]

        if s not in db_map:
            db_map[s] = {"name": s, "schemas": {}}
        if t not in db_map[s]["schemas"]:
            db_map[s]["schemas"][t] = {"name": t, "tables": {}}
        if t not in db_map[s]["schemas"][t]["tables"]:
            db_map[s]["schemas"][t]["tables"][t] = {"name": t, "columns": []}
        db_map[s]["schemas"][t]["tables"][t]["columns"].append({"name": c, "type": dt})

    for db_name, db_data in db_map.items():
        db_obj = {"name": db_name, "schemas": []}
        for schema_name, schema_data in db_data["schemas"].items():
            sch_obj = {"name": schema_name, "tables": []}
            for table_name, table_data in schema_data["tables"].items():
                sch_obj["tables"].append(table_data)
            db_obj["schemas"].append(sch_obj)
        schema["databases"].append(db_obj)
    return schema

SCHEMA_EXTRACTORS = {
    "postgresql": get_postgres_schema,
    "mysql": get_mysql_schema,
    "mariadb": get_mysql_schema,
    "mssql": get_mssql_schema,
    "oracle": get_oracle_schema,
    "snowflake": get_snowflake_schema,
}

# ========== QUERY EXECUTORS ==========

def execute_sql_query(conn, query: str, page: int = 1, page_size: int = 100):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        if cursor.description:
            columns = [desc[0] for desc in cursor.description]
            offset = (page - 1) * page_size
            all_rows = cursor.fetchall()
            total = len(all_rows)
            rows = all_rows[offset:offset + page_size]
            data = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    val = row[i] if not isinstance(row, dict) else row.get(col)
                    if hasattr(val, 'isoformat'):
                        val = val.isoformat()
                    row_dict[col] = val
                data.append(row_dict)
            return {"columns": columns, "data": data, "total": total, "page": page, "page_size": page_size}
        else:
            conn.commit()
            return {"columns": [], "data": [], "total": 0, "page": page, "page_size": page_size, "message": f"Affected {cursor.rowcount} rows"}
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()

def execute_mongodb_query(client, db_name, query: str, page: int = 1, page_size: int = 100):
    db = client[db_name]
    # Simple query: collection.find() or collection.aggregate()
    try:
        result = eval(f"db.{query}")  # Simplified - in production use safer parsing
        docs = list(result.limit(page_size).skip((page - 1) * page_size))
        total = result.count() if hasattr(result, 'count') else len(docs)
        if docs:
            columns = list(docs[0].keys())
            data = [{k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v for k, v in doc.items()} for doc in docs]
        else:
            columns, data = [], []
        return {"columns": columns, "data": data, "total": total, "page": page, "page_size": page_size}
    except Exception as e:
        raise e

# ========== EXPORT FUNCTIONS ==========

def export_to_csv(data, columns):
    import csv
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    writer.writerows(data)
    output.seek(0)
    return output.getvalue().encode('utf-8')

def export_to_excel(data, columns):
    if not PANDAS_AVAILABLE:
        raise HTTPException(500, "Pandas not installed for Excel export")
    df = pd.DataFrame(data, columns=columns)
    output = BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)
    return output.getvalue()

def export_to_json(data, columns):
    return json.dumps(data, indent=2, default=str).encode('utf-8')

def export_to_parquet(data, columns):
    if not PANDAS_AVAILABLE:
        raise HTTPException(500, "Pandas not installed for Parquet export")
    df = pd.DataFrame(data, columns=columns)
    output = BytesIO()
    df.to_parquet(output, index=False)
    output.seek(0)
    return output.getvalue()

# ========== AI SQL GENERATION ==========

async def generate_sql_with_groq(natural_language: str, db_type: str, schema_hint: str = ""):
    if not GROQ_API_KEY:
        raise HTTPException(500, "GROQ_API_KEY not configured")

    system_prompt = f"""You are an expert SQL query generator. Convert natural language to valid {db_type} SQL.
Rules:
- Return ONLY the SQL query, no explanations
- Use proper {db_type} syntax
- Include semicolons where appropriate
- If schema info is provided, use correct table and column names"""

    user_prompt = f"Schema context: {schema_hint}\n\nQuery: {natural_language}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "llama3-70b-8192",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                "temperature": 0.1,
                "max_tokens": 2048
            }
        )
        result = response.json()
        if "choices" in result and len(result["choices"]) > 0:
            sql = result["choices"][0]["message"]["content"].strip()
            sql = sql.replace("```sql", "").replace("```", "").strip()
            return sql
        else:
            raise HTTPException(500, f"AI generation failed: {result.get('error', 'Unknown error')}")

# ========== FASTAPI APP ==========

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # Cleanup active connections
    for key, conn in active_connections.items():
        try:
            if hasattr(conn, 'close'):
                conn.close()
            elif hasattr(conn, 'disconnect'):
                conn.disconnect()
        except:
            pass
    active_connections.clear()

app = FastAPI(
    title="Migranix Data Platform API",
    description="Backend for multi-database connectivity, querying, and AI SQL generation",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== HEALTH ==========
@app.get("/")
async def root():
    return {"status": "Migranix Data Platform API is running", "version": "1.0.0"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

# ========== CONNECTION MANAGEMENT ==========

@app.post("/api/connections/test")
async def test_connection(req: TestConnectionRequest):
    """Test a database connection without saving"""
    try:
        db_type = req.db_type.lower()
        if db_type not in CONNECTORS:
            raise HTTPException(400, f"Unsupported database type: {db_type}")

        connector = CONNECTORS[db_type]
        conn = connector(req.credentials)

        # Test by getting server version or simple query
        if db_type in ["postgresql", "mysql", "mariadb", "redshift"]:
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            cursor.close()
        elif db_type == "mssql":
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            cursor.close()
        elif db_type == "oracle":
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM v$version")
            version = cursor.fetchone()[0]
            cursor.close()
        elif db_type == "snowflake":
            cursor = conn.cursor()
            cursor.execute("SELECT current_version()")
            version = cursor.fetchone()[0]
            cursor.close()
        elif db_type == "mongodb":
            version = conn.server_info()["version"]
        elif db_type == "bigquery":
            version = "BigQuery " + conn.project
        else:
            version = "Connected successfully"

        # Close test connection
        if hasattr(conn, 'close'):
            conn.close()

        return {"success": True, "message": "Connection successful", "version": str(version)[:100]}

    except Exception as e:
        raise HTTPException(400, detail=f"Connection failed: {str(e)}")

@app.post("/api/connections/save")
async def save_connection(req: SaveConnectionRequest):
    """Save a connection profile to Supabase (encrypted)"""
    try:
        # First test the connection
        db_type = req.db_type.lower()
        if db_type in CONNECTORS:
            connector = CONNECTORS[db_type]
            conn = connector(req.credentials)
            if hasattr(conn, 'close'):
                conn.close()

        # Encrypt credentials
        creds_json = req.credentials.json()
        encrypted = encrypt_text(creds_json)

        # Save to Supabase
        data = {
            "user_id": req.user_id,
            "name": req.name,
            "db_type": db_type,
            "credentials_encrypted": encrypted,
            "created_at": datetime.utcnow().isoformat(),
            "last_used": datetime.utcnow().isoformat()
        }

        result = supabase.table("saved_connections").insert(data).execute()

        return {
            "success": True,
            "message": "Connection saved successfully",
            "connection_id": result.data[0]["id"] if result.data else None
        }

    except Exception as e:
        raise HTTPException(400, detail=f"Failed to save connection: {str(e)}")

@app.get("/api/connections/{user_id}")
async def list_connections(user_id: str):
    """List all saved connections for a user"""
    try:
        result = supabase.table("saved_connections").select("id, name, db_type, created_at, last_used").eq("user_id", user_id).order("last_used", desc=True).execute()
        return {"connections": result.data}
    except Exception as e:
        raise HTTPException(400, detail=str(e))

@app.delete("/api/connections/{connection_id}")
async def delete_connection(connection_id: str, user_id: str):
    """Delete a saved connection"""
    try:
        supabase.table("saved_connections").delete().eq("id", connection_id).eq("user_id", user_id).execute()
        return {"success": True, "message": "Connection deleted"}
    except Exception as e:
        raise HTTPException(400, detail=str(e))

# ========== CONNECT & SCHEMA ==========

@app.post("/api/connections/{connection_id}/connect")
async def connect_to_db(connection_id: str, user_id: str):
    """Establish an active connection from a saved profile"""
    try:
        result = supabase.table("saved_connections").select("*").eq("id", connection_id).eq("user_id", user_id).single().execute()

        if not result.data:
            raise HTTPException(404, "Connection not found")

        conn_data = result.data
        db_type = conn_data["db_type"]
        encrypted_creds = conn_data["credentials_encrypted"]

        # Decrypt and parse credentials
        decrypted = decrypt_text(encrypted_creds)
        creds = ConnectionCredentials.parse_raw(decrypted)

        # Establish connection
        connector = CONNECTORS.get(db_type)
        if not connector:
            raise HTTPException(400, f"Connector for {db_type} not available")

        conn = connector(creds)

        # Store in active pool
        active_connections[f"{user_id}:{connection_id}"] = conn

        # Update last_used
        supabase.table("saved_connections").update({"last_used": datetime.utcnow().isoformat()}).eq("id", connection_id).execute()

        # Get schema
        schema = {}
        if db_type in SCHEMA_EXTRACTORS:
            schema = SCHEMA_EXTRACTORS[db_type](conn)
        elif db_type == "mongodb":
            schema = get_mongodb_schema(conn, creds.database)
        elif db_type == "bigquery":
            schema = get_bigquery_schema(conn)

        return {
            "success": True,
            "message": f"Connected to {db_type}",
            "connection_id": connection_id,
            "schema": schema
        }

    except Exception as e:
        raise HTTPException(400, detail=f"Connection failed: {str(e)}")

@app.get("/api/connections/{connection_id}/schema")
async def get_schema(connection_id: str, user_id: str):
    """Get schema for an active connection"""
    try:
        key = f"{user_id}:{connection_id}"
        if key not in active_connections:
            raise HTTPException(400, "Connection not active. Please connect first.")

        conn = active_connections[key]
        result = supabase.table("saved_connections").select("db_type").eq("id", connection_id).single().execute()
        db_type = result.data["db_type"]

        schema = {}
        if db_type in SCHEMA_EXTRACTORS:
            schema = SCHEMA_EXTRACTORS[db_type](conn)
        elif db_type == "mongodb":
            # Need db_name from stored connection
            creds_data = supabase.table("saved_connections").select("credentials_encrypted").eq("id", connection_id).single().execute()
            creds = ConnectionCredentials.parse_raw(decrypt_text(creds_data.data["credentials_encrypted"]))
            schema = get_mongodb_schema(conn, creds.database)
        elif db_type == "bigquery":
            schema = get_bigquery_schema(conn)

        return {"schema": schema}

    except Exception as e:
        raise HTTPException(400, detail=str(e))

# ========== QUERY EXECUTION ==========

@app.post("/api/query/execute")
async def execute_query(req: ExecuteQueryRequest):
    """Execute a SQL query on an active connection"""
    try:
        key = f"{req.user_id}:{req.connection_id}"
        if key not in active_connections:
            raise HTTPException(400, "Connection not active. Please connect first.")

        conn = active_connections[key]
        result = supabase.table("saved_connections").select("db_type").eq("id", req.connection_id).single().execute()
        db_type = result.data["db_type"]

        if db_type == "mongodb":
            # Handle MongoDB queries differently
            creds_data = supabase.table("saved_connections").select("credentials_encrypted").eq("id", req.connection_id).single().execute()
            creds = ConnectionCredentials.parse_raw(decrypt_text(creds_data.data["credentials_encrypted"]))
            result = execute_mongodb_query(conn, creds.database, req.query, req.page, req.page_size)
        else:
            result = execute_sql_query(conn, req.query, req.page, req.page_size)

        return result

    except Exception as e:
        raise HTTPException(400, detail=f"Query execution failed: {str(e)}")

# ========== AI SQL GENERATION ==========

@app.post("/api/ai/generate-sql")
async def ai_generate_sql(req: AISQLRequest):
    """Generate SQL from natural language using Groq"""
    try:
        sql = await generate_sql_with_groq(req.natural_language, req.db_type, req.schema_hint or "")
        return {"success": True, "sql": sql}
    except Exception as e:
        raise HTTPException(400, detail=str(e))

# ========== EXPORT ==========

@app.post("/api/query/export")
async def export_query_results(req: ExportRequest):
    """Export query results to various formats"""
    try:
        key = f"{req.user_id}:{req.connection_id}"
        if key not in active_connections:
            raise HTTPException(400, "Connection not active")

        conn = active_connections[key]
        result = supabase.table("saved_connections").select("db_type").eq("id", req.connection_id).single().execute()
        db_type = result.data["db_type"]

        if db_type == "mongodb":
            creds_data = supabase.table("saved_connections").select("credentials_encrypted").eq("id", req.connection_id).single().execute()
            creds = ConnectionCredentials.parse_raw(decrypt_text(creds_data.data["credentials_encrypted"]))
            query_result = execute_mongodb_query(conn, creds.database, req.query, 1, 10000)
        else:
            query_result = execute_sql_query(conn, req.query, 1, 10000)

        data = query_result["data"]
        columns = query_result["columns"]

        fmt = req.format.lower()
        if fmt == "csv":
            content = export_to_csv(data, columns)
            media_type = "text/csv"
            filename = "query_results.csv"
        elif fmt == "excel":
            content = export_to_excel(data, columns)
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename = "query_results.xlsx"
        elif fmt == "json":
            content = export_to_json(data, columns)
            media_type = "application/json"
            filename = "query_results.json"
        elif fmt == "parquet":
            content = export_to_parquet(data, columns)
            media_type = "application/octet-stream"
            filename = "query_results.parquet"
        else:
            raise HTTPException(400, f"Unsupported format: {fmt}")

        return StreamingResponse(
            BytesIO(content),
            media_type=media_type,
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        raise HTTPException(400, detail=f"Export failed: {str(e)}")

# ========== FILE UPLOAD ==========

@app.post("/api/files/upload")
async def upload_file(file: UploadFile = File(...), file_type: str = Form(...)):
    """Upload and parse a file (CSV, Excel, JSON, Parquet, etc.)"""
    try:
        content = await file.read()

        if file_type in ["csv"]:
            if not PANDAS_AVAILABLE:
                raise HTTPException(500, "Pandas required for CSV parsing")
            df = pd.read_csv(BytesIO(content))
        elif file_type in ["excel", "xlsx", "xls"]:
            if not PANDAS_AVAILABLE:
                raise HTTPException(500, "Pandas required for Excel parsing")
            df = pd.read_excel(BytesIO(content))
        elif file_type == "json":
            if not PANDAS_AVAILABLE:
                raise HTTPException(500, "Pandas required for JSON parsing")
            df = pd.read_json(BytesIO(content))
        elif file_type == "parquet":
            if not PANDAS_AVAILABLE:
                raise HTTPException(500, "Pandas required for Parquet parsing")
            df = pd.read_parquet(BytesIO(content))
        else:
            raise HTTPException(400, f"Unsupported file type: {file_type}")

        # Convert to preview format
        preview = df.head(100).to_dict(orient='records')
        columns = list(df.columns)

        return {
            "success": True,
            "filename": file.filename,
            "rows": len(df),
            "columns": columns,
            "preview": preview,
            "dtypes": {col: str(df[col].dtype) for col in columns}
        }

    except Exception as e:
        raise HTTPException(400, detail=f"File upload failed: {str(e)}")

# ========== RUN ==========

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
