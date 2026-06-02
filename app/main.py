"""
Migranix Backend API — Universal Data Connector
FastAPI service supporting 14 database/warehouse/NoSQL sources
"""

import os
import re
import json
import uuid
import io
from datetime import datetime
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

# ========== ALL OPTIONAL IMPORTS (graceful degradation) ==========
try: import psycopg2
except ImportError: psycopg2 = None

try: import pymysql
except ImportError: pymysql = None

try: import pyodbc
except ImportError: pyodbc = None

import sqlite3

try:
    from sqlalchemy import create_engine, text, inspect
    from sqlalchemy.pool import NullPool
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    create_engine = None
    NullPool = None
    SQLAlchemyError = Exception

try: import snowflake.connector as snowflake_connector
except ImportError: snowflake_connector = None

try: from google.cloud import bigquery as gcp_bigquery
except ImportError: gcp_bigquery = None

try: from google.cloud import storage as gcp_storage
except ImportError: gcp_storage = None

try: import pymongo
except ImportError: pymongo = None

try: from cassandra.cluster import Cluster
except ImportError: Cluster = None

try: from cassandra.auth import PlainTextAuthProvider
except ImportError: PlainTextAuthProvider = None

try: import boto3
except ImportError: boto3 = None

try: from databricks import sql as databricks_sql
except ImportError: databricks_sql = None

try: from azure.storage.blob import BlobServiceClient
except ImportError: BlobServiceClient = None

try: from azure.cosmos import CosmosClient
except ImportError: CosmosClient = None

try: import oracledb as cx_Oracle   # python-oracledb thin mode — no Oracle Client needed
except ImportError: cx_Oracle = None

try: import ibm_db, ibm_db_sa
except ImportError: ibm_db = None

try: import fastavro
except ImportError: fastavro = None

import pandas as pd
import httpx

# ========== CONFIG ==========
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
connections: Dict[str, Dict[str, Any]] = {}

# ========== POWER BI-STYLE ERROR FORMATTER ==========
def power_bi_error(db_type: str, exc: Exception) -> dict:
    """Convert any exception into a structured error matching Power BI's pattern."""
    msg = str(exc)
    low = msg.lower()
    likely_cause = None
    fix = None

    if 'no module named' in low or 'cannot import name' in low:
        likely_cause = f"The {db_type} driver is not installed on the server"
        fix = "Contact support — driver missing"
    elif 'connection refused' in low or 'could not connect' in low or 'failed to connect' in low or 'cannot reach' in low:
        likely_cause = "Server unreachable from Migranix's cloud network"
        fix = "Ensure the database has a PUBLIC endpoint and the firewall allows Render IPs. Localhost/private IPs will not work."
    elif 'getaddrinfo' in low or 'no such host' in low or 'name or service not known' in low or 'nodename nor servname' in low:
        likely_cause = "Hostname cannot be resolved"
        fix = "Check the server address — typo or missing domain suffix"
    elif 'timeout' in low or 'timed out' in low:
        likely_cause = "Server did not respond"
        fix = "Server may be down, firewalled, or behind a VPN that Migranix cannot reach"
    elif 'authentication failed' in low or 'login failed' in low or 'password authentication' in low or 'incorrect password' in low:
        likely_cause = "Username or password is incorrect"
        fix = "Verify credentials in the database admin panel"
    elif 'access denied' in low or 'permission denied' in low or 'insufficient privilege' in low:
        likely_cause = "User lacks permission on this database"
        fix = "Grant CONNECT / USAGE / SELECT privileges to your user"
    elif 'ssl' in low or 'certificate' in low or 'tls' in low:
        likely_cause = "SSL/TLS handshake failed"
        fix = "Toggle the SSL setting or check server certificate validity"
    elif ('database' in low and ('does not exist' in low or 'not exist' in low or 'unknown database' in low)) or 'no such file' in low:
        likely_cause = "Database or file not found"
        fix = "Verify the database/file name — case-sensitive on some servers"
    elif 'role' in low and 'does not exist' in low:
        likely_cause = "Role/user does not exist on the server"
        fix = "Create the user first or check the username spelling"
    elif 'odbc driver' in low and 'not found' in low:
        likely_cause = "ODBC driver missing on server"
        fix = "Contact support — ODBC driver needs reinstall"
    elif 'service_name' in low or 'tns' in low:
        likely_cause = "Oracle service name not recognized"
        fix = "Verify the Service Name (not SID) — check listener.ora on the Oracle server"
    elif 'invalid bucket name' in low or 'nosuchbucket' in low:
        likely_cause = "S3 bucket name is wrong or doesn't exist"
        fix = "Check exact bucket name in AWS console"
    elif 'signatureversion' in low or 'signature does not match' in low:
        likely_cause = "AWS credentials are wrong"
        fix = "Re-generate access key / secret key in IAM"
    else:
        likely_cause = None
        fix = None

    return {
        "message": f"Could not connect to {db_type}",
        "details": msg[:500],
        "likely_cause": likely_cause,
        "fix": fix
    }


def raise_pbi(db_type: str, exc: Exception):
    """Raise HTTPException with Power BI-style structured error."""
    raise HTTPException(status_code=400, detail=power_bi_error(db_type, exc))


# ========== PYDANTIC MODELS ==========
class DBCredentials(BaseModel):
    type: str
    credentials: Dict[str, Any]

class QueryRequest(BaseModel):
    session: Optional[str] = None
    session_id: Optional[str] = None
    query: str

    def get_session(self):
        return self.session_id or self.session or ""

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
    stage: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None


# ========== HELPERS ==========
def _get(creds: dict, *keys, default=None):
    """Try multiple keys, return first non-empty value, then default."""
    for k in keys:
        v = creds.get(k)
        if v not in (None, "", " "):
            return v
    return default


def _truthy(val) -> bool:
    if isinstance(val, bool): return val
    if isinstance(val, str): return val.lower() in ('true', '1', 'yes', 'on')
    return bool(val)


# ========== CONNECTION MANAGERS ==========
class ConnectionManager:

    @staticmethod
    def create_postgresql(creds: dict):
        host = _get(creds, 'host')
        if not host: raise ValueError("Server is required")
        port = _get(creds, 'port', default=5432)
        database = _get(creds, 'database', default='postgres')
        username = _get(creds, 'username')
        password = _get(creds, 'password', default='')
        if not username: raise ValueError("Username is required")

        from urllib.parse import quote_plus
        dsn = f"postgresql+psycopg2://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/{database}"
        if _truthy(creds.get('ssl')):
            dsn += "?sslmode=require"
        engine = create_engine(dsn, poolclass=NullPool, connect_args={"connect_timeout": 15})
        return dsn, engine

    @staticmethod
    def create_mysql(creds: dict):
        host = _get(creds, 'host')
        if not host: raise ValueError("Server is required")
        port = _get(creds, 'port', default=3306)
        database = _get(creds, 'database', default='')
        username = _get(creds, 'username')
        password = _get(creds, 'password', default='')
        if not username: raise ValueError("Username is required")

        from urllib.parse import quote_plus
        db_part = f"/{database}" if database else ""
        dsn = f"mysql+pymysql://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}{db_part}"
        engine = create_engine(dsn, poolclass=NullPool, connect_args={"connect_timeout": 15})
        return dsn, engine

    @staticmethod
    def create_sqlserver(creds: dict):
        if pyodbc is None:
            raise ImportError("pyodbc driver not installed")
        host = _get(creds, 'host')
        if not host: raise ValueError("Server is required")
        port = _get(creds, 'port', default=1433)
        database = _get(creds, 'database', default='master')
        auth_type = _get(creds, 'auth_type', default='sql')
        trust = 'yes' if _truthy(creds.get('trust_server_certificate')) else 'no'

        driver = "{ODBC Driver 18 for SQL Server}"
        base = f"DRIVER={driver};SERVER={host},{port};DATABASE={database};TrustServerCertificate={trust};Encrypt=yes;"

        if auth_type == 'sql':
            username = _get(creds, 'username')
            password = _get(creds, 'password', default='')
            if not username: raise ValueError("SQL Username is required")
            odbc_str = base + f"UID={username};PWD={password};"
        elif auth_type == 'aad_password':
            username = _get(creds, 'aad_username')
            password = _get(creds, 'aad_password', default='')
            if not username: raise ValueError("Azure AD Username is required")
            odbc_str = base + f"UID={username};PWD={password};Authentication=ActiveDirectoryPassword;"
        elif auth_type == 'service_principal':
            tenant_id = _get(creds, 'tenant_id')
            client_id = _get(creds, 'client_id')
            client_secret = _get(creds, 'client_secret')
            if not (tenant_id and client_id and client_secret):
                raise ValueError("Tenant ID, Client ID, and Client Secret are required")
            odbc_str = base + f"UID={client_id}@{tenant_id};PWD={client_secret};Authentication=ActiveDirectoryServicePrincipal;"
        else:
            raise ValueError(f"Unknown SQL Server auth_type: {auth_type}")

        from urllib.parse import quote_plus
        dsn = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
        engine = create_engine(dsn, poolclass=NullPool)
        return dsn, engine

    @staticmethod
    def create_oracle(creds: dict):
        if cx_Oracle is None:
            raise ImportError("python-oracledb not installed")
        host = _get(creds, 'host')
        if not host: raise ValueError("Host is required")
        port = _get(creds, 'port', default=1521)
        service_name = _get(creds, 'service_name', 'service', default='ORCL')
        username = _get(creds, 'username')
        password = _get(creds, 'password', default='')
        if not username: raise ValueError("System User is required")

        # oracledb thin mode — no Oracle Instant Client needed
        from urllib.parse import quote_plus
        dsn = f"oracle+oracledb://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/?service_name={service_name}"
        engine = create_engine(dsn, poolclass=NullPool,
                               connect_args={"thick_mode": False})  # explicit thin mode
        return dsn, engine

    @staticmethod
    def create_db2(creds: dict):
        if ibm_db is None:
            raise ImportError("ibm_db driver not installed")
        host = _get(creds, 'host')
        if not host: raise ValueError("Host is required")
        port = _get(creds, 'port', default=50000)
        database = _get(creds, 'database')
        if not database: raise ValueError("Database is required")
        username = _get(creds, 'username')
        password = _get(creds, 'password', default='')
        if not username: raise ValueError("Username is required")

        from urllib.parse import quote_plus
        dsn = f"db2+ibm_db://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/{database}"
        engine = create_engine(dsn, poolclass=NullPool)
        return dsn, engine

    @staticmethod
    def create_sqlite(creds: dict):
        filepath = _get(creds, 'filepath')
        if not filepath: raise ValueError("File path is required")
        dsn = f"sqlite:///{filepath}"
        engine = create_engine(dsn, poolclass=NullPool)
        return dsn, engine

    @staticmethod
    def create_snowflake(creds: dict):
        if snowflake_connector is None:
            raise ImportError("snowflake-connector-python not installed")
        account = _get(creds, 'account')
        if not account: raise ValueError("Account is required")
        warehouse = _get(creds, 'warehouse')
        if not warehouse: raise ValueError("Warehouse is required")

        # Strip URL parts if user pasted full URL
        account = account.replace('https://', '').replace('.snowflakecomputing.com', '').rstrip('/')

        conn_args = {
            'account': account,
            'warehouse': warehouse,
        }
        if creds.get('database'): conn_args['database'] = creds['database']
        if creds.get('schema'): conn_args['schema'] = creds['schema']
        if creds.get('role'): conn_args['role'] = creds['role']

        auth_type = _get(creds, 'auth_type', default='password')

        if auth_type == 'password':
            username = _get(creds, 'username')
            password = _get(creds, 'password', default='')
            if not username: raise ValueError("Username is required")
            conn_args['user'] = username
            conn_args['password'] = password
        elif auth_type == 'keypair':
            username = _get(creds, 'username')
            private_key_pem = _get(creds, 'private_key')
            if not username: raise ValueError("Username is required")
            if not private_key_pem: raise ValueError("Private Key PEM is required")
            try:
                from cryptography.hazmat.primitives import serialization
                passphrase = creds.get('private_key_passphrase') or None
                pk = serialization.load_pem_private_key(
                    private_key_pem.encode(),
                    password=passphrase.encode() if passphrase else None
                )
                pkb = pk.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
                conn_args['user'] = username
                conn_args['private_key'] = pkb
            except Exception as e:
                raise ValueError(f"Invalid private key: {e}")
        elif auth_type == 'token':
            token = _get(creds, 'token')
            if not token: raise ValueError("OAuth Token is required")
            conn_args['authenticator'] = 'oauth'
            conn_args['token'] = token
        else:
            raise ValueError(f"Unknown Snowflake auth_type: {auth_type}")

        conn = snowflake_connector.connect(**conn_args)
        return "snowflake", conn

    @staticmethod
    def create_bigquery(creds: dict):
        if gcp_bigquery is None:
            raise ImportError("google-cloud-bigquery not installed")
        project = _get(creds, 'project')
        if not project: raise ValueError("Project ID is required")

        auth_type = _get(creds, 'auth_type', default='service_account')

        if auth_type == 'service_account':
            credentials_json = _get(creds, 'credentials_json')
            if not credentials_json: raise ValueError("Service Account JSON is required")
            from google.oauth2 import service_account
            try:
                info = json.loads(credentials_json) if isinstance(credentials_json, str) else credentials_json
            except json.JSONDecodeError as e:
                raise ValueError(f"Service Account JSON is invalid: {e}")
            credentials = service_account.Credentials.from_service_account_info(info)
            client = gcp_bigquery.Client(project=project, credentials=credentials)
        elif auth_type == 'oauth_token':
            from google.oauth2.credentials import Credentials as OAuthCreds
            token = _get(creds, 'oauth_token')
            if not token: raise ValueError("OAuth Token is required")
            credentials = OAuthCreds(token=token)
            client = gcp_bigquery.Client(project=project, credentials=credentials)
        else:
            raise ValueError(f"Unknown BigQuery auth_type: {auth_type}")

        return "bigquery", client

    @staticmethod
    def create_redshift(creds: dict):
        host = _get(creds, 'host')
        if not host: raise ValueError("Host is required")
        port = _get(creds, 'port', default=5439)
        database = _get(creds, 'database', default='dev')
        username = _get(creds, 'username')
        password = _get(creds, 'password', default='')
        if not username: raise ValueError("Username is required")

        from urllib.parse import quote_plus
        dsn = f"postgresql+psycopg2://{quote_plus(username)}:{quote_plus(password)}@{host}:{port}/{database}?sslmode=require"
        engine = create_engine(dsn, poolclass=NullPool, connect_args={"connect_timeout": 15})
        return dsn, engine

    @staticmethod
    def create_databricks(creds: dict):
        if databricks_sql is None:
            raise ImportError("databricks-sql-connector not installed")
        host = _get(creds, 'server_hostname')
        if not host: raise ValueError("Server Hostname is required")
        http_path = _get(creds, 'http_path')
        if not http_path: raise ValueError("HTTP Path is required")
        token = _get(creds, 'token')
        if not token: raise ValueError("Access Token is required")
        catalog = _get(creds, 'catalog')
        schema = _get(creds, 'schema')

        kwargs = {
            'server_hostname': host.replace('https://', '').rstrip('/'),
            'http_path': http_path,
            'access_token': token,
        }
        if catalog: kwargs['catalog'] = catalog
        if schema: kwargs['schema'] = schema

        conn = databricks_sql.connect(**kwargs)
        return "databricks", conn

    @staticmethod
    def create_mongodb(creds: dict):
        if pymongo is None:
            raise ImportError("pymongo not installed")
        auth_type = _get(creds, 'auth_type', default='uri')

        if auth_type == 'uri':
            uri = _get(creds, 'uri')
            if not uri: raise ValueError("Connection URI is required")
            client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=15000)
        else:  # manual
            host = _get(creds, 'host')
            if not host: raise ValueError("Host is required")
            port = int(_get(creds, 'port', default=27017))
            username = _get(creds, 'username')
            password = _get(creds, 'password', default='')
            if username:
                client = pymongo.MongoClient(host=host, port=port, username=username, password=password, serverSelectionTimeoutMS=15000)
            else:
                client = pymongo.MongoClient(host=host, port=port, serverSelectionTimeoutMS=15000)

        # Force connection check
        client.admin.command('ping')
        db_name = _get(creds, 'database', default='admin')
        db = client[db_name]
        return "mongodb", db

    @staticmethod
    def create_cassandra(creds: dict):
        if Cluster is None:
            raise ImportError("cassandra-driver not installed")
        hosts_str = _get(creds, 'hosts')
        if not hosts_str: raise ValueError("Seed Nodes are required")
        hosts = [h.strip() for h in hosts_str.split(',') if h.strip()]
        port = int(_get(creds, 'port', default=9042))
        keyspace = _get(creds, 'keyspace')
        username = _get(creds, 'username')
        password = _get(creds, 'password', default='')

        if username and PlainTextAuthProvider:
            auth = PlainTextAuthProvider(username=username, password=password)
            cluster = Cluster(hosts, port=port, auth_provider=auth, connect_timeout=15)
        else:
            cluster = Cluster(hosts, port=port, connect_timeout=15)
        session = cluster.connect(keyspace) if keyspace else cluster.connect()
        return "cassandra", session

    @staticmethod
    def create_dynamodb(creds: dict):
        if boto3 is None:
            raise ImportError("boto3 not installed")
        region = _get(creds, 'region')
        if not region: raise ValueError("AWS Region is required")
        access_key = _get(creds, 'access_key')
        secret_key = _get(creds, 'secret_key')
        if not access_key: raise ValueError("Access Key is required")
        if not secret_key: raise ValueError("Secret Key is required")

        kwargs = dict(region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        session_token = _get(creds, 'session_token')
        if session_token:
            kwargs['aws_session_token'] = session_token

        client = boto3.client('dynamodb', **kwargs)
        # Sanity check
        client.list_tables(Limit=1)
        return "dynamodb", client

    @staticmethod
    def create_cosmosdb(creds: dict):
        uri = _get(creds, 'uri')
        if not uri: raise ValueError("Endpoint URI is required")
        auth_type = _get(creds, 'auth_type', default='key')

        if CosmosClient is None:
            raise ImportError("azure-cosmos not installed")

        if auth_type == 'key':
            key = _get(creds, 'key')
            if not key: raise ValueError("Primary Key is required")
            client = CosmosClient(uri, credential=key)
        elif auth_type == 'token':
            token = _get(creds, 'token')
            if not token: raise ValueError("Bearer Token is required")
            client = CosmosClient(uri, credential={'type': 'AccessToken', 'token': token})
        else:
            raise ValueError(f"Unknown Cosmos DB auth_type: {auth_type}")

        # Sanity check
        list(client.list_databases())
        return "cosmosdb", client


# ========== FASTAPI APP ==========
app = FastAPI(title="Migranix API", version="2.1")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://migranix.in", "https://www.migranix.in", "http://localhost:3000", "http://localhost:5500"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"status": "Migranix API v2.1", "timestamp": datetime.utcnow().isoformat()}


# ========== TEST CONNECTION ==========
@app.post("/api/test-connection")
async def test_connection(req: DBCredentials):
    db_type = req.type
    try:
        manager = ConnectionManager()
        creator = getattr(manager, f"create_{db_type}", None)
        if not creator:
            raise ValueError(f"Unsupported database type: {db_type}")

        result = creator(req.credentials)
        if isinstance(result, tuple):
            first, second = result
            if isinstance(first, str) and second is not None and hasattr(second, 'connect'):
                # SQLAlchemy engine
                with second.connect() as conn:
                    conn.execute(text("SELECT 1"))
                second.dispose()
            # Non-engine connections were already pinged inside their creators
        return {"success": True, "message": "Connection successful"}
    except (ValueError, ImportError) as e:
        raise HTTPException(400, detail=power_bi_error(db_type, e))
    except Exception as e:
        raise HTTPException(400, detail=power_bi_error(db_type, e))


# ========== CONNECT (creates session) ==========
@app.post("/api/connect")
async def connect(req: DBCredentials):
    db_type = req.type
    try:
        manager = ConnectionManager()
        creator = getattr(manager, f"create_{db_type}", None)
        if not creator:
            raise ValueError(f"Unsupported database type: {db_type}")

        result = creator(req.credentials)
        session_id = str(uuid.uuid4())

        if isinstance(result, tuple):
            first, second = result
            if isinstance(first, str) and second is not None and hasattr(second, 'connect') and hasattr(second, 'dispose'):
                connections[session_id] = {
                    "type": db_type, "engine": second, "dsn": first,
                    "created_at": datetime.utcnow().isoformat()
                }
            else:
                connections[session_id] = {
                    "type": db_type, "connection": second,
                    "created_at": datetime.utcnow().isoformat()
                }
        return {"success": True, "session_id": session_id}
    except (ValueError, ImportError) as e:
        raise HTTPException(400, detail=power_bi_error(db_type, e))
    except Exception as e:
        raise HTTPException(400, detail=power_bi_error(db_type, e))


# ========== SCHEMA ==========
@app.get("/api/schema")
async def get_schema(session: str):
    if session not in connections:
        raise HTTPException(404, detail={"message": "Session expired", "details": "Please reconnect", "likely_cause": "Server restarted or session timed out", "fix": "Click 'Connect to Data' again"})
    conn = connections[session]
    db_type = conn.get("type", "")
    try:
        if "engine" in conn:
            engine = conn["engine"]
            inspector = inspect(engine)
            schema_data = []
            try:
                schemas = inspector.get_schema_names() or ['public']
            except Exception:
                schemas = ['public']
            db_name = conn.get("dsn", "").split('/')[-1].split('?')[0] or db_type
            db_info = {"name": db_name, "schemas": []}
            for s in schemas[:10]:
                try:
                    tbls = inspector.get_table_names(schema=s)
                except Exception:
                    tbls = inspector.get_table_names()
                s_info = {"name": s, "tables": []}
                for t in tbls[:100]:
                    try:
                        cols = inspector.get_columns(t, schema=s)
                    except Exception:
                        cols = inspector.get_columns(t)
                    s_info["tables"].append({
                        "name": t,
                        "columns": [{"name": c["name"], "type": str(c["type"])} for c in cols[:80]]
                    })
                db_info["schemas"].append(s_info)
            schema_data.append(db_info)
            return {"success": True, "schema": schema_data}
        else:
            return {"success": True, "schema": [{"name": db_type, "tables": []}]}
    except Exception as e:
        raise HTTPException(400, detail=power_bi_error(db_type, e))


# ========== QUERY ==========
@app.post("/api/query")
async def execute_query(req: QueryRequest):
    sid = req.get_session()
    if sid not in connections:
        raise HTTPException(404, detail={"message": "Session expired", "details": "Please reconnect", "likely_cause": "Session timed out", "fix": "Reconnect to your data source"})
    conn = connections[sid]
    db_type = conn.get("type", "")
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
                    return {"success": True, "columns": [], "results": [], "message": "Query executed"}
        else:
            raise ValueError("Direct SQL not supported for this connection type (use the native query tool)")
    except HTTPException:
        raise
    except Exception as e:
        # Clean SQLAlchemy error noise
        err_msg = str(e)
        if "(sqlite3." in err_msg:
            try: err_msg = err_msg.split("(sqlite3.")[1].split(")")[1].strip()
            except: pass
        raise HTTPException(400, detail=power_bi_error(db_type, type(e)(err_msg)))


# ========== FILE UPLOAD ==========
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
            except Exception:
                raw = json.loads(content)
                if isinstance(raw, list):
                    df = pd.json_normalize(raw, sep='_')
                elif isinstance(raw, dict):
                    nested_list = None
                    for v in raw.values():
                        if isinstance(v, list):
                            nested_list = v; break
                    df = pd.json_normalize(nested_list or [raw], sep='_')
                else:
                    df = pd.DataFrame([raw])
        elif type == 'parquet':
            df = pd.read_parquet(io.BytesIO(content))
        elif type == 'xml':
            df = pd.read_xml(io.BytesIO(content))
        elif type == 'avro':
            if fastavro is None: raise HTTPException(400, "Avro support not installed")
            reader = fastavro.reader(io.BytesIO(content))
            df = pd.DataFrame([r for r in reader])
        else:
            raise HTTPException(400, f"Unsupported file type: {type}")

        engine = create_engine("sqlite:///:memory:", poolclass=NullPool)
        raw_name = file.filename.rsplit('.', 1)[0]
        table_name = re.sub(r'[^a-z0-9_]', '', raw_name.lower().replace('-', '_').replace(' ', '_').replace('.', '_'))
        if not table_name or table_name[0].isdigit():
            table_name = 't_' + table_name
        df.to_sql(table_name, engine, index=False)

        session_id = str(uuid.uuid4())
        connections[session_id] = {
            "type": "file", "engine": engine, "tables": [table_name],
            "table_name": table_name, "created_at": datetime.utcnow().isoformat()
        }
        return {
            "success": True, "session_id": session_id,
            "tables": [{"name": table_name, "columns": [{"name": c, "type": str(d)} for c, d in df.dtypes.items()]}]
        }
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(400, detail=power_bi_error("file", e))


# ========== CLOUD STORAGE ==========
@app.post("/api/test-cloud")
async def test_cloud(creds: CloudCreds):
    provider = creds.provider
    try:
        if provider == 's3':
            if boto3 is None: raise ImportError("boto3 not installed")
            s3 = boto3.client('s3', aws_access_key_id=creds.access_key, aws_secret_access_key=creds.secret_key, region_name=creds.region)
            s3.list_objects_v2(Bucket=creds.bucket, Prefix=creds.prefix or '', MaxKeys=1)
        elif provider == 'gcs':
            if gcp_storage is None: raise ImportError("google-cloud-storage not installed")
            if creds.credentials_json:
                from google.oauth2 import service_account
                info = json.loads(creds.credentials_json)
                credentials = service_account.Credentials.from_service_account_info(info)
                client = gcp_storage.Client(project=creds.project, credentials=credentials)
            else:
                client = gcp_storage.Client(project=creds.project)
            list(client.bucket(creds.bucket).list_blobs(max_results=1))
        elif provider == 'azure':
            if BlobServiceClient is None: raise ImportError("azure-storage-blob not installed")
            url = f"https://{creds.account}.blob.core.windows.net"
            client = BlobServiceClient(account_url=url, credential=creds.sas_token)
            list(client.list_containers(results_per_page=1))
        elif provider == 'snowflake_stage':
            if snowflake_connector is None: raise ImportError("snowflake-connector-python not installed")
            conn = snowflake_connector.connect(
                account=creds.account, user=creds.username, password=creds.password,
            )
            conn.cursor().execute(f"LIST @{creds.stage}")
        else:
            raise ValueError(f"Unknown provider: {provider}")
        return {"success": True}
    except Exception as e:
        raise HTTPException(400, detail=power_bi_error(provider, e))


@app.post("/api/connect-cloud")
async def connect_cloud(creds: CloudCreds):
    provider = creds.provider
    try:
        tables = []
        if provider == 's3' and boto3:
            s3 = boto3.client('s3', aws_access_key_id=creds.access_key, aws_secret_access_key=creds.secret_key, region_name=creds.region)
            response = s3.list_objects_v2(Bucket=creds.bucket, Prefix=creds.prefix or '')
            tables = [{"name": obj['Key'], "columns": []} for obj in response.get('Contents', [])[:50]]
        return {"success": True, "tables": tables}
    except Exception as e:
        raise HTTPException(400, detail=power_bi_error(provider, e))


# ========== EXPORT ==========
@app.post("/api/export")
async def export_data(req: ExportRequest):
    try:
        df = pd.DataFrame(req.results)
        if req.format == 'csv':
            out = io.StringIO(); df.to_csv(out, index=False)
            return StreamingResponse(io.BytesIO(out.getvalue().encode()), media_type="text/csv",
                                     headers={"Content-Disposition": "attachment; filename=export.csv"})
        elif req.format == 'json':
            out = io.BytesIO(df.to_json(orient='records').encode())
            return StreamingResponse(out, media_type="application/json")
        elif req.format == 'excel':
            out = io.BytesIO(); df.to_excel(out, index=False, engine='openpyxl'); out.seek(0)
            return StreamingResponse(out, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        elif req.format == 'parquet':
            out = io.BytesIO(); df.to_parquet(out, index=False); out.seek(0)
            return StreamingResponse(out, media_type="application/octet-stream")
        raise ValueError("Unsupported format")
    except Exception as e:
        raise HTTPException(400, detail=power_bi_error("export", e))


# ========== AI FEATURES (Groq) ==========
async def call_ai(prompt, system_prompt="", max_tokens=2048):
    key = GROQ_API_KEY
    if not key:
        raise HTTPException(400, detail={"message": "AI not configured", "details": "GROQ_API_KEY not set", "likely_cause": "Server config", "fix": "Set GROQ_API_KEY in Render env vars"})
    messages = []
    if system_prompt: messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"model": "llama-3.3-70b-versatile", "messages": messages, "temperature": 0.1, "max_tokens": max_tokens}
        )
        data = resp.json()
        if resp.status_code != 200:
            raise HTTPException(400, detail=power_bi_error("Groq AI", Exception(data.get("error", {}).get("message", "Groq API error"))))
        return data.get("choices", [{}])[0].get("message", {}).get("content", "")


@app.post("/api/ai-sql")
async def generate_sql(request: dict):
    prompt = request.get("prompt", "")
    schema_context = request.get("schema_context", "")
    system = f"""Convert natural language to SQL. {('Schema: ' + schema_context) if schema_context else ''}
Rules: Return ONLY the SQL, no markdown/explanation. Use standard SQL syntax."""
    sql = (await call_ai(prompt, system)).strip()
    if sql.startswith("```"): sql = sql.split("\n", 1)[1] if "\n" in sql else sql[3:]
    if sql.endswith("```"): sql = sql[:-3]
    return {"success": True, "sql": sql.strip()}


@app.post("/api/ai-explain")
async def ai_explain(request: dict):
    sql = request.get("sql", "")
    if not sql: raise HTTPException(400, "No SQL provided")
    result = await call_ai(f"Explain this SQL in plain English:\n\n{sql}", "You are a data expert.")
    return {"success": True, "explanation": result}


@app.post("/api/ai-optimize")
async def ai_optimize(request: dict):
    sql = request.get("sql", "")
    if not sql: raise HTTPException(400, "No SQL provided")
    result = await call_ai(f"Optimize this SQL:\n{sql}\n\nProvide: 1) Optimized SQL 2) Explanation 3) Performance estimate", "You are a database performance expert.")
    return {"success": True, "optimization": result}


@app.post("/api/ai-clean")
async def ai_clean(request: dict):
    sid = request.get("session_id", "")
    table_name = request.get("table_name", "")
    if sid not in connections: raise HTTPException(400, "No active connection")
    conn = connections[sid]
    if conn["type"] != "file":
        raise HTTPException(400, "Auto-clean only works on uploaded files")
    try:
        import numpy as np
        engine = conn["engine"]
        df = pd.read_sql(f"SELECT * FROM '{table_name}'", engine)
        orig_rows, orig_cols = len(df), len(df.columns)
        changes = []
        for col in df.select_dtypes(include=['object']).columns:
            before = df[col].copy()
            df[col] = df[col].str.strip()
            n = (before != df[col]).sum()
            if n > 0: changes.append(f"Trimmed whitespace in '{col}': {n} values")
        nulls = ['NA', 'N/A', 'null', 'NULL', 'None', 'none', '-', '', 'undefined', 'nil', '#N/A', 'NaN']
        for col in df.select_dtypes(include=['object']).columns:
            mask = df[col].isin(nulls)
            if mask.sum() > 0:
                df.loc[mask, col] = np.nan
                changes.append(f"Converted {mask.sum()} null-like values in '{col}'")
        empty = df.isnull().all(axis=1).sum()
        if empty > 0: df = df.dropna(how='all'); changes.append(f"Removed {empty} empty rows")
        dupes = df.duplicated().sum()
        if dupes > 0: df = df.drop_duplicates(); changes.append(f"Removed {dupes} duplicate rows")
        for col in df.select_dtypes(include=['object']).columns:
            try:
                converted = pd.to_numeric(df[col], errors='coerce')
                if df[col].notna().sum() > 0 and converted.notna().sum() / df[col].notna().sum() >= 0.85:
                    df[col] = converted; changes.append(f"Converted '{col}' to numeric")
            except: pass
        cleaned = f"{table_name}_cleaned"
        df.to_sql(cleaned, engine, index=False, if_exists='replace')
        return {"success": True, "original_rows": orig_rows, "cleaned_rows": len(df),
                "removed_rows": orig_rows - len(df), "original_columns": orig_cols,
                "changes": changes, "cleaned_table": cleaned}
    except Exception as e:
        raise HTTPException(400, detail=power_bi_error("clean", e))


@app.post("/api/ai-profile")
async def ai_profile(request: dict):
    sid = request.get("session_id", "")
    table = request.get("table_name", "")
    if sid not in connections: raise HTTPException(400, "No active connection")
    conn = connections[sid]
    if conn["type"] != "file":
        raise HTTPException(400, "AI profiling supports file uploads only")
    try:
        engine = conn["engine"]
        with engine.connect() as c:
            total = c.execute(text(f"SELECT COUNT(*) FROM '{table}'")).fetchone()[0]
            cols_info = [{"name": r[1], "type": r[2]} for r in c.execute(text(f"PRAGMA table_info('{table}')")).fetchall()]
            sample = c.execute(text(f"SELECT * FROM '{table}' LIMIT 5"))
            sample_rows = [dict(zip(list(sample.keys()), row)) for row in sample.fetchall()]
        col_summary = "\n".join([f"- {c['name']} ({c['type']})" for c in cols_info])
        prompt = f"""Profile this dataset. Return ONLY valid JSON:
Table: {table}, Rows: {total}, Columns:
{col_summary}
Sample: {json.dumps(sample_rows[:3], default=str)}

{{
  "quality_score": <0-100>,
  "summary": "<overview>",
  "column_analysis": [{{"column": "name", "detected_type": "...", "issues": [], "suggestion": "..."}}],
  "data_issues": [{{"severity": "high|medium|low", "issue": "...", "affected_rows": 0, "fix": "..."}}],
  "cleaning_sql": ["..."]
}}"""
        result = (await call_ai(prompt, "You are a data quality expert. Return ONLY JSON.")).strip()
        if result.startswith("```"): result = result.split("\n", 1)[1]
        if result.endswith("```"): result = result[:-3]
        try: profile = json.loads(result.strip())
        except: profile = {"quality_score": 0, "summary": result, "column_analysis": [], "data_issues": [], "cleaning_sql": []}
        profile.update({"total_rows": total, "total_columns": len(cols_info), "columns": cols_info})
        return {"success": True, "profile": profile}
    except HTTPException: raise
    except Exception as e:
        raise HTTPException(400, detail=power_bi_error("profile", e))


@app.post("/api/ai-migration-plan")
async def ai_migration_plan(request: dict):
    src = request.get("source_type", "")
    tgt = request.get("target_type", "Snowflake")
    info = request.get("schema_info", "")
    prompt = f"""Create migration plan from {src} to {tgt}. Schema: {info}
Include: 1) Pre-migration checklist 2) Schema mapping 3) Type conversions 4) Timeline 5) Risk assessment 6) Validation steps 7) Target DDL."""
    result = await call_ai(prompt, "You are a senior data migration architect.")
    return {"success": True, "plan": result}


@app.on_event("shutdown")
async def shutdown():
    for conn in connections.values():
        try:
            if "engine" in conn: conn["engine"].dispose()
        except: pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
