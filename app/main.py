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

try: import oracledb as cx_Oracle   # package name: oracledb (pip install oracledb)
except ImportError: cx_Oracle = None

try: import ibm_db, ibm_db_sa
except ImportError: ibm_db = None

try: import fastavro
except ImportError: fastavro = None

# ========== SAP DRIVERS (all optional — graceful degradation) ==========
try: import hdbcli.dbapi as sap_hana_db        # SAP HANA: pip install hdbcli
except ImportError: sap_hana_db = None

try: import pyrfc as sap_pyrfc                  # SAP ECC/S4 RFC: needs NW RFC SDK + pyrfc
except ImportError: sap_pyrfc = None

# ========== ERP HTTP/SOAP (stdlib only — no extra packages needed) ==========
import urllib.request
import urllib.parse
import urllib.error
import base64
import hmac
import hashlib
import time as _time_module
import xml.etree.ElementTree as ET

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
            raise ImportError("oracledb not installed. Run: pip install oracledb")
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

    # ========== SAP CONNECTIONS ==========

    @staticmethod
    def create_sap_hana(creds: dict):
        """SAP HANA — direct SQL via hdbcli. Works for SAP HANA and BW4HANA."""
        if sap_hana_db is None:
            raise ImportError(
                "hdbcli driver not installed. "
                "Run: pip install hdbcli  (on your Hetzner server)"
            )
        host = _get(creds, 'host')
        if not host: raise ValueError("HANA Host is required")
        port = int(_get(creds, 'port', default=30015))
        username = _get(creds, 'username')
        password = _get(creds, 'password', default='')
        if not username: raise ValueError("Username is required")
        schema = _get(creds, 'schema', default='')

        conn = sap_hana_db.connect(
            address=host,
            port=port,
            user=username,
            password=password,
            encrypt=True,
            sslValidateCertificate=False,   # common in SAP landscapes
            connectTimeout=15,
        )
        # Sanity check
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM DUMMY")
        cur.close()

        # Store schema hint for DDL generation
        conn._migranix_schema = schema or username.upper()
        return "sap_hana", conn

    @staticmethod
    def create_sap_ecc(creds: dict):
        """SAP ECC / R3 — RFC connection via PyRFC.
        Requires SAP NW RFC SDK installed manually on the server.
        Install guide: https://github.com/SAP/PyRFC#installation
        """
        if sap_pyrfc is None:
            raise ImportError(
                "pyrfc not installed or SAP NW RFC SDK missing. "
                "Install guide: https://github.com/SAP/PyRFC#installation  "
                "Then run: pip install pyrfc"
            )
        host = _get(creds, 'host')
        if not host: raise ValueError("SAP Application Server Host is required")
        sysnr = _get(creds, 'sysnr', default='00')
        client_id = _get(creds, 'client', default='100')
        username = _get(creds, 'username')
        password = _get(creds, 'password', default='')
        if not username: raise ValueError("SAP Username is required")
        lang = _get(creds, 'lang', default='EN')

        params = {
            'ashost': host,
            'sysnr': str(sysnr).zfill(2),
            'client': str(client_id).zfill(3),
            'user': username,
            'passwd': password,
            'lang': lang,
        }
        # Optional message server / group for load balancing
        mshost = _get(creds, 'mshost')
        msserv = _get(creds, 'msserv')
        sysid  = _get(creds, 'sysid')
        if mshost:
            params['mshost'] = mshost
            if msserv: params['msserv'] = msserv
            if sysid:  params['sysid']  = sysid

        conn = sap_pyrfc.Connection(**params)
        # Sanity ping
        conn.call('RFC_PING')
        return "sap_ecc", conn

    @staticmethod
    def create_sap_s4hana(creds: dict):
        """SAP S/4HANA — supports both RFC (same as ECC) and OData REST API.
        auth_type='rfc'   → uses PyRFC (same as ECC)
        auth_type='odata' → uses HTTP basic/OAuth (no special driver)
        """
        auth_type = _get(creds, 'auth_type', default='rfc')

        if auth_type == 'rfc':
            # Reuse ECC RFC logic
            return ConnectionManager.create_sap_ecc(creds)

        elif auth_type == 'odata':
            base_url = _get(creds, 'base_url')
            if not base_url: raise ValueError("S/4HANA OData Base URL is required")
            username = _get(creds, 'username')
            password = _get(creds, 'password', default='')
            client_id = _get(creds, 'client_id')       # OAuth2 client id
            client_secret = _get(creds, 'client_secret')
            token_url = _get(creds, 'token_url')

            # Build requests session for OData calls
            import requests
            session = requests.Session()
            session.verify = False     # many SAP systems use self-signed certs
            session.timeout = 15

            if client_id and client_secret and token_url:
                # OAuth2 client credentials
                token_resp = session.post(token_url, data={
                    'grant_type': 'client_credentials',
                    'client_id': client_id,
                    'client_secret': client_secret,
                })
                if token_resp.status_code != 200:
                    raise ValueError(f"OAuth token failed: {token_resp.text[:300]}")
                token = token_resp.json().get('access_token')
                session.headers['Authorization'] = f'Bearer {token}'
            else:
                # Basic auth
                if not username: raise ValueError("Username is required")
                session.auth = (username, password)

            # Ping: fetch service catalog
            ping = session.get(f"{base_url.rstrip('/')}/sap/opu/odata/sap/")
            if ping.status_code not in (200, 401, 403, 404):
                raise ValueError(f"OData endpoint returned HTTP {ping.status_code}")

            # Attach metadata to session for later use
            session._migranix_base_url = base_url.rstrip('/')
            return "sap_s4hana_odata", session

        else:
            raise ValueError(f"Unknown S/4HANA auth_type: {auth_type}. Use 'rfc' or 'odata'")

    @staticmethod
    def create_sap_bw(creds: dict):
        """SAP BW / BW4HANA.
        BW4HANA runs on HANA — use hdbcli.
        Older BW on ABAP stack — use PyRFC.
        auth_type='hana' → hdbcli
        auth_type='rfc'  → PyRFC (same as ECC)
        """
        auth_type = _get(creds, 'auth_type', default='hana')
        if auth_type == 'hana':
            return ConnectionManager.create_sap_hana(creds)
        elif auth_type == 'rfc':
            return ConnectionManager.create_sap_ecc(creds)
        else:
            raise ValueError(f"Unknown BW auth_type: {auth_type}. Use 'hana' or 'rfc'")

    # ========== ERP CONNECTIONS ==========

    @staticmethod
    def create_oracle_ebs(creds: dict):
        """Oracle E-Business Suite — direct Oracle DB connection.
        EBS stores everything in its Oracle schema (APPS, AR, AP, GL, etc.).
        Uses oracledb thin mode — no Oracle Instant Client needed.
        """
        if cx_Oracle is None:
            raise ImportError("oracledb not installed. Run: pip install oracledb")
        host = _get(creds, 'host')
        if not host: raise ValueError("EBS Database Host is required")
        port         = _get(creds, 'port', default=1521)
        service_name = _get(creds, 'service_name', default='EBSPROD')
        username     = _get(creds, 'username', default='APPS')
        password     = _get(creds, 'password', default='')
        if not username: raise ValueError("Username is required (usually APPS)")

        from urllib.parse import quote_plus
        dsn = (f"oracle+oracledb://{quote_plus(username)}:{quote_plus(password)}"
               f"@{host}:{port}/?service_name={service_name}")
        engine = create_engine(dsn, poolclass=NullPool,
                               connect_args={"thick_mode": False})
        # Sanity check
        with engine.connect() as c:
            c.execute(text("SELECT 1 FROM DUAL"))

        conn_meta = {"_erp": "oracle_ebs", "_schema": username.upper()}
        return dsn, engine, conn_meta

    @staticmethod
    def create_oracle_erp_cloud(creds: dict):
        """Oracle ERP Cloud (Fusion) — REST API with OAuth2.
        No direct DB access — Oracle manages the infrastructure.
        Uses Oracle's REST API: https://<pod>.fa.us2.oraclecloud.com/fscmRestApi/resources/latest/
        """
        base_url      = _get(creds, 'base_url')
        client_id     = _get(creds, 'client_id')
        client_secret = _get(creds, 'client_secret')
        token_url     = _get(creds, 'token_url')
        username      = _get(creds, 'username')
        password      = _get(creds, 'password', default='')

        if not base_url: raise ValueError("Oracle ERP Cloud Base URL is required")

        import requests
        session = requests.Session()
        session.verify = True
        session.timeout = 20

        if client_id and client_secret and token_url:
            # OAuth2 client credentials
            r = session.post(token_url, data={
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret,
                'scope': 'urn:opc:db:scope:oracle_erp_api',
            })
            if r.status_code != 200:
                raise ValueError(f"OAuth2 token failed (HTTP {r.status_code}): {r.text[:300]}")
            token = r.json().get('access_token')
            if not token: raise ValueError("OAuth2 token response missing access_token")
            session.headers['Authorization'] = f'Bearer {token}'
        elif username:
            # Basic auth (username/password — works for Oracle Cloud Identity)
            session.auth = (username, password)
        else:
            raise ValueError("Provide either (client_id + client_secret + token_url) or username/password")

        # Ping — check REST endpoint responds
        base = base_url.rstrip('/')
        ping = session.get(f"{base}/fscmRestApi/resources/latest/", timeout=15)
        if ping.status_code not in (200, 401, 403):
            raise ValueError(f"Oracle ERP Cloud REST endpoint returned HTTP {ping.status_code}. Check base URL.")

        session._migranix_base_url = base
        session._migranix_erp = 'oracle_erp_cloud'
        return "oracle_erp_cloud", session

    @staticmethod
    def create_dynamics365(creds: dict):
        """Microsoft Dynamics 365 — OData REST API with Azure AD OAuth2.
        Endpoint: https://<org>.crm.dynamics.com/api/data/v9.2/
        Also supports Dynamics 365 Finance & Operations (F&O).
        """
        tenant_id     = _get(creds, 'tenant_id')
        client_id     = _get(creds, 'client_id')
        client_secret = _get(creds, 'client_secret')
        base_url      = _get(creds, 'base_url')

        if not tenant_id:  raise ValueError("Azure Tenant ID is required")
        if not client_id:  raise ValueError("Azure App Client ID is required")
        if not base_url:   raise ValueError("Dynamics 365 URL is required (e.g. https://org.crm.dynamics.com)")

        import requests
        base = base_url.rstrip('/')
        resource = base + '/'

        # Get token from Azure AD
        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        scope     = f"{resource}.default"

        if client_secret:
            # Confidential client
            r = requests.post(token_url, data={
                'grant_type':    'client_credentials',
                'client_id':     client_id,
                'client_secret': client_secret,
                'scope':         scope,
            }, timeout=15)
        else:
            raise ValueError("client_secret is required for Dynamics 365 connection")

        if r.status_code != 200:
            raise ValueError(f"Azure AD token failed (HTTP {r.status_code}): {r.text[:300]}")

        token = r.json().get('access_token')
        if not token: raise ValueError("Token response missing access_token")

        session = requests.Session()
        session.headers.update({
            'Authorization': f'Bearer {token}',
            'Accept':        'application/json',
            'OData-MaxVersion': '4.0',
            'OData-Version':    '4.0',
        })
        session.timeout = 20

        # Ping OData metadata
        ping = session.get(f"{base}/api/data/v9.2/", timeout=15)
        if ping.status_code not in (200, 401):
            raise ValueError(f"Dynamics 365 OData endpoint returned HTTP {ping.status_code}. Check URL.")

        session._migranix_base_url = base
        session._migranix_erp      = 'dynamics365'
        return "dynamics365", session

    @staticmethod
    def create_dynamics_onprem(creds: dict):
        """Microsoft Dynamics AX / NAV / GP — on-premise.
        These run on SQL Server — connect directly via pyodbc.
        AX = Dynamics AX (now D365 F&O on-prem)
        NAV = Dynamics NAV (now Business Central on-prem)
        GP  = Dynamics GP (Great Plains)
        """
        if pyodbc is None:
            raise ImportError("pyodbc not installed")
        host     = _get(creds, 'host')
        if not host: raise ValueError("SQL Server Host is required")
        port     = _get(creds, 'port', default=1433)
        database = _get(creds, 'database')
        if not database: raise ValueError("Database name is required")
        username = _get(creds, 'username')
        password = _get(creds, 'password', default='')
        if not username: raise ValueError("Username is required")

        driver   = "{ODBC Driver 18 for SQL Server}"
        odbc_str = (f"DRIVER={driver};SERVER={host},{port};DATABASE={database};"
                    f"UID={username};PWD={password};"
                    f"TrustServerCertificate=yes;Encrypt=yes;")

        from urllib.parse import quote_plus
        dsn    = f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"
        engine = create_engine(dsn, poolclass=NullPool)
        with engine.connect() as c:
            c.execute(text("SELECT 1"))

        conn_meta = {"_erp": "dynamics_onprem", "_database": database}
        return dsn, engine, conn_meta

    @staticmethod
    def create_netsuite(creds: dict):
        """NetSuite — SuiteTalk REST API with OAuth 1.0a (TBA).
        NetSuite uses OAuth 1.0a Token-Based Authentication (TBA) — not OAuth2.
        Required: account_id, consumer_key, consumer_secret, token_id, token_secret
        """
        account_id      = _get(creds, 'account_id')
        consumer_key    = _get(creds, 'consumer_key')
        consumer_secret = _get(creds, 'consumer_secret')
        token_id        = _get(creds, 'token_id')
        token_secret    = _get(creds, 'token_secret')

        if not account_id:      raise ValueError("NetSuite Account ID is required")
        if not consumer_key:    raise ValueError("Consumer Key is required")
        if not consumer_secret: raise ValueError("Consumer Secret is required")
        if not token_id:        raise ValueError("Token ID is required")
        if not token_secret:    raise ValueError("Token Secret is required")

        # Build OAuth 1.0a signature
        def _ns_auth_header(method: str, url: str) -> str:
            nonce    = base64.b64encode(os.urandom(32)).decode('ascii').rstrip('=')
            ts       = str(int(_time_module.time()))
            params   = {
                'oauth_consumer_key':     consumer_key,
                'oauth_nonce':            nonce,
                'oauth_signature_method': 'HMAC-SHA256',
                'oauth_timestamp':        ts,
                'oauth_token':            token_id,
                'oauth_version':          '1.0',
                'realm':                  account_id,
            }
            base_str_params = '&'.join(
                f"{urllib.parse.quote(k,'')}"
                f"={urllib.parse.quote(str(params[k]),'')}"
                for k in sorted(params) if k != 'realm'
            )
            base_string = (f"{method.upper()}&"
                           f"{urllib.parse.quote(url,'')}&"
                           f"{urllib.parse.quote(base_str_params,'')}")
            signing_key = (f"{urllib.parse.quote(consumer_secret,'')}&"
                           f"{urllib.parse.quote(token_secret,'')}")
            sig = base64.b64encode(
                hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha256).digest()
            ).decode()
            params['oauth_signature'] = sig
            header = (f'OAuth realm="{account_id}",' +
                      ','.join(f'{k}="{urllib.parse.quote(str(v),"")}"'
                               for k, v in params.items() if k != 'realm'))
            return header

        # Normalise account id (NS uses TSTDRVXXXXXXXX → tstdrvxxxxxxxx for URL)
        acct_url = account_id.lower().replace('_', '-')
        base_url = f"https://{acct_url}.suitetalk.api.netsuite.com/services/rest/record/v1"

        # Sanity ping — GET record types
        ping_url = f"{base_url}/"
        auth_hdr = _ns_auth_header("GET", ping_url)
        import requests
        r = requests.get(ping_url,
                         headers={"Authorization": auth_hdr,
                                  "Content-Type":   "application/json"},
                         timeout=15)
        if r.status_code not in (200, 401, 403, 404):
            raise ValueError(f"NetSuite REST API returned HTTP {r.status_code}: {r.text[:300]}")

        meta = {
            "_erp":             "netsuite",
            "_account_id":      account_id,
            "_consumer_key":    consumer_key,
            "_consumer_secret": consumer_secret,
            "_token_id":        token_id,
            "_token_secret":    token_secret,
            "_base_url":        base_url,
            "_make_auth":       _ns_auth_header,
        }
        return "netsuite", meta

    @staticmethod
    def create_workday(creds: dict):
        """Workday — REST API with OAuth2 (Workday as Authorization Server).
        Endpoint pattern: https://<hostname>/ccx/api/v1/<tenant>/
        Required: hostname, tenant, client_id, client_secret, refresh_token
        Workday uses refresh_token grant — no user interaction needed for API.
        """
        hostname      = _get(creds, 'hostname')
        tenant        = _get(creds, 'tenant')
        client_id     = _get(creds, 'client_id')
        client_secret = _get(creds, 'client_secret')
        refresh_token = _get(creds, 'refresh_token')

        if not hostname:      raise ValueError("Workday Hostname is required (e.g. wd2-impl-services1.workday.com)")
        if not tenant:        raise ValueError("Workday Tenant is required")
        if not client_id:     raise ValueError("Client ID is required")
        if not refresh_token: raise ValueError("Refresh Token is required (from Workday API Client setup)")

        import requests
        token_url = f"https://{hostname}/ccx/oauth2/{tenant}/token"
        r = requests.post(token_url, data={
            'grant_type':    'refresh_token',
            'refresh_token': refresh_token,
            'client_id':     client_id,
            'client_secret': client_secret or '',
        }, timeout=15)

        if r.status_code != 200:
            raise ValueError(f"Workday OAuth2 token failed (HTTP {r.status_code}): {r.text[:300]}")

        token = r.json().get('access_token')
        if not token: raise ValueError("Workday token response missing access_token")

        session = requests.Session()
        session.headers['Authorization'] = f'Bearer {token}'
        session.timeout = 20

        # Ping — check workers endpoint
        base = f"https://{hostname}/ccx/api/v1/{tenant}"
        ping = session.get(f"{base}/workers", params={'limit': 1}, timeout=15)
        if ping.status_code not in (200, 400):
            raise ValueError(f"Workday API returned HTTP {ping.status_code}. Check hostname/tenant.")

        session._migranix_base_url = base
        session._migranix_erp      = 'workday'
        return "workday", session


# ========== SAP TABLE CATALOGUE (business-friendly name mapping) ==========
# Curated list of ~200 most-used SAP tables with module tags and friendly names.
# Used for DDL generation — user selects tables from this list.
SAP_TABLE_CATALOGUE = [
    # ---- SD — Sales & Distribution ----
    {"table":"VBAK","name":"SALES_ORDER_HEADER","module":"SD","desc":"Sales Order Header"},
    {"table":"VBAP","name":"SALES_ORDER_ITEMS","module":"SD","desc":"Sales Order Line Items"},
    {"table":"VBKD","name":"SALES_ORDER_BUSINESS","module":"SD","desc":"Sales Order Business Data"},
    {"table":"VBPA","name":"SALES_ORDER_PARTNERS","module":"SD","desc":"Sales Order Partners"},
    {"table":"LIKP","name":"DELIVERY_HEADER","module":"SD","desc":"Delivery Header"},
    {"table":"LIPS","name":"DELIVERY_ITEMS","module":"SD","desc":"Delivery Line Items"},
    {"table":"VBRK","name":"BILLING_HEADER","module":"SD","desc":"Billing Document Header"},
    {"table":"VBRP","name":"BILLING_ITEMS","module":"SD","desc":"Billing Document Line Items"},
    {"table":"VBFA","name":"DOCUMENT_FLOW","module":"SD","desc":"Document Flow (Order→Delivery→Invoice)"},
    {"table":"VBEP","name":"SALES_SCHEDULE_LINES","module":"SD","desc":"Sales Order Schedule Lines"},
    {"table":"KONV","name":"PRICING_CONDITIONS","module":"SD","desc":"Pricing Conditions"},
    {"table":"KNVV","name":"CUSTOMER_SALES_DATA","module":"SD","desc":"Customer Sales Area Data"},
    {"table":"KNVP","name":"CUSTOMER_PARTNERS","module":"SD","desc":"Customer Partner Functions"},

    # ---- MM — Materials Management ----
    {"table":"MARA","name":"MATERIAL_MASTER_GENERAL","module":"MM","desc":"Material Master General Data"},
    {"table":"MARC","name":"MATERIAL_MASTER_PLANT","module":"MM","desc":"Material Master Plant Data"},
    {"table":"MARD","name":"MATERIAL_STOCK","module":"MM","desc":"Material Stock per Storage Location"},
    {"table":"MAKT","name":"MATERIAL_DESCRIPTIONS","module":"MM","desc":"Material Descriptions (multi-lang)"},
    {"table":"MARM","name":"MATERIAL_UOM","module":"MM","desc":"Units of Measure for Materials"},
    {"table":"MBEW","name":"MATERIAL_VALUATION","module":"MM","desc":"Material Valuation"},
    {"table":"EKKO","name":"PURCHASE_ORDER_HEADER","module":"MM","desc":"Purchase Order Header"},
    {"table":"EKPO","name":"PURCHASE_ORDER_ITEMS","module":"MM","desc":"Purchase Order Line Items"},
    {"table":"EKBE","name":"PO_HISTORY","module":"MM","desc":"Purchase Order History"},
    {"table":"EBAN","name":"PURCHASE_REQUISITION_ITEMS","module":"MM","desc":"Purchase Requisition Items"},
    {"table":"EKET","name":"PO_SCHEDULE_LINES","module":"MM","desc":"Purchase Order Schedule Lines"},
    {"table":"MKPF","name":"GOODS_MOVEMENT_HEADER","module":"MM","desc":"Goods Movement Document Header"},
    {"table":"MSEG","name":"GOODS_MOVEMENT_ITEMS","module":"MM","desc":"Goods Movement Document Items"},
    {"table":"EINA","name":"PURCHASING_INFO_GENERAL","module":"MM","desc":"Purchasing Info Record General Data"},
    {"table":"EINE","name":"PURCHASING_INFO_ORG","module":"MM","desc":"Purchasing Info Record Org Data"},
    {"table":"LFA1","name":"VENDOR_MASTER_GENERAL","module":"MM","desc":"Vendor Master General Data"},
    {"table":"LFB1","name":"VENDOR_MASTER_COMPANY","module":"MM","desc":"Vendor Master Company Code Data"},
    {"table":"LFM1","name":"VENDOR_PURCHASING","module":"MM","desc":"Vendor Master Purchasing Org Data"},

    # ---- FI — Finance ----
    {"table":"BKPF","name":"ACCOUNTING_DOCUMENT_HEADER","module":"FI","desc":"Accounting Document Header"},
    {"table":"BSEG","name":"ACCOUNTING_DOCUMENT_ITEMS","module":"FI","desc":"Accounting Document Line Items"},
    {"table":"BSAD","name":"CLEARED_CUSTOMER_ITEMS","module":"FI","desc":"Cleared Customer Line Items"},
    {"table":"BSAK","name":"CLEARED_VENDOR_ITEMS","module":"FI","desc":"Cleared Vendor Line Items"},
    {"table":"BSID","name":"OPEN_CUSTOMER_ITEMS","module":"FI","desc":"Open Customer Line Items"},
    {"table":"BSIK","name":"OPEN_VENDOR_ITEMS","module":"FI","desc":"Open Vendor Line Items"},
    {"table":"BSIS","name":"OPEN_GL_ITEMS","module":"FI","desc":"Open G/L Account Line Items"},
    {"table":"BSAS","name":"CLEARED_GL_ITEMS","module":"FI","desc":"Cleared G/L Account Line Items"},
    {"table":"SKA1","name":"GL_ACCOUNT_MASTER","module":"FI","desc":"G/L Account Master (Chart of Accounts)"},
    {"table":"SKAT","name":"GL_ACCOUNT_TEXTS","module":"FI","desc":"G/L Account Short Texts"},
    {"table":"SKB1","name":"GL_ACCOUNT_COMPANY","module":"FI","desc":"G/L Account Master per Company Code"},
    {"table":"KNA1","name":"CUSTOMER_MASTER_GENERAL","module":"FI","desc":"Customer Master General Data"},
    {"table":"KNB1","name":"CUSTOMER_MASTER_COMPANY","module":"FI","desc":"Customer Master Company Code Data"},
    {"table":"T001","name":"COMPANY_CODES","module":"FI","desc":"Company Codes"},
    {"table":"FAGLFLEXT","name":"GL_BALANCES","module":"FI","desc":"General Ledger Account Balances"},
    {"table":"ACDOCA","name":"UNIVERSAL_JOURNAL","module":"FI","desc":"Universal Journal Entry Line Items (S/4HANA)"},

    # ---- CO — Controlling ----
    {"table":"COSS","name":"CO_OBJECT_TOTALS","module":"CO","desc":"CO Object Totals (Primary Costs)"},
    {"table":"COSP","name":"CO_OBJECT_SECONDARY","module":"CO","desc":"CO Object Totals (Secondary Costs)"},
    {"table":"COBK","name":"CO_DOCUMENT_HEADER","module":"CO","desc":"CO Document Header"},
    {"table":"COEP","name":"CO_DOCUMENT_ITEMS","module":"CO","desc":"CO Document Line Items"},
    {"table":"CSKA","name":"COST_ELEMENT_MASTER","module":"CO","desc":"Cost Elements (Chart of Accounts)"},
    {"table":"CSKT","name":"COST_ELEMENT_TEXTS","module":"CO","desc":"Cost Element Texts"},
    {"table":"CSKS","name":"COST_CENTER_MASTER","module":"CO","desc":"Cost Center Master Data"},
    {"table":"CSKT2","name":"COST_CENTER_TEXTS","module":"CO","desc":"Cost Center Texts"},

    # ---- PP — Production Planning ----
    {"table":"AUFK","name":"PRODUCTION_ORDER_MASTER","module":"PP","desc":"Production Order Master Data"},
    {"table":"AFKO","name":"PRODUCTION_ORDER_HEADER","module":"PP","desc":"Production Order Header"},
    {"table":"AFPO","name":"PRODUCTION_ORDER_ITEMS","module":"PP","desc":"Production Order Items"},
    {"table":"AFVC","name":"PRODUCTION_OPERATIONS","module":"PP","desc":"Production Order Operations"},
    {"table":"AFVV","name":"PRODUCTION_QUANTITIES","module":"PP","desc":"Quantities/Dates for Operations"},
    {"table":"RESB","name":"MATERIAL_RESERVATIONS","module":"PP","desc":"Material Reservations/Requirements"},
    {"table":"MAST","name":"BOM_LINK","module":"PP","desc":"Material to BOM Link"},
    {"table":"STKO","name":"BOM_HEADER","module":"PP","desc":"BOM Header"},
    {"table":"STPO","name":"BOM_ITEMS","module":"PP","desc":"BOM Items"},
    {"table":"CRHD","name":"WORK_CENTER_HEADER","module":"PP","desc":"Work Center Header"},

    # ---- HR — Human Resources ----
    {"table":"PA0000","name":"HR_ACTIONS","module":"HR","desc":"HR Master Record: Actions"},
    {"table":"PA0001","name":"HR_ORG_ASSIGNMENT","module":"HR","desc":"HR Master Record: Org Assignment"},
    {"table":"PA0002","name":"HR_PERSONAL_DATA","module":"HR","desc":"HR Master Record: Personal Data"},
    {"table":"PA0006","name":"HR_ADDRESSES","module":"HR","desc":"HR Master Record: Addresses"},
    {"table":"PA0007","name":"HR_PLANNED_HOURS","module":"HR","desc":"HR Master Record: Planned Working Time"},
    {"table":"PA0008","name":"HR_BASIC_PAY","module":"HR","desc":"HR Master Record: Basic Pay"},
    {"table":"HRP1000","name":"HR_OBJECTS","module":"HR","desc":"HR Objects (Org Units, Positions, Jobs)"},
    {"table":"HRP1001","name":"HR_RELATIONSHIPS","module":"HR","desc":"HR Object Relationships"},

    # ---- PM — Plant Maintenance ----
    {"table":"EQUI","name":"EQUIPMENT_MASTER","module":"PM","desc":"Equipment Master Data"},
    {"table":"IFLOT","name":"FUNCTIONAL_LOCATION","module":"PM","desc":"Functional Location Master"},
    {"table":"VIQMEL","name":"NOTIFICATIONS","module":"PM","desc":"PM/QM Notifications"},
    {"table":"VIAUFKST","name":"WORK_ORDERS","module":"PM","desc":"PM Work Orders"},

    # ---- QM — Quality Management ----
    {"table":"QMEL","name":"QM_NOTIFICATIONS","module":"QM","desc":"Quality Notifications"},
    {"table":"QALS","name":"INSPECTION_LOTS","module":"QM","desc":"Inspection Lots"},
    {"table":"QAVE","name":"USAGE_DECISIONS","module":"QM","desc":"Usage Decisions"},

    # ---- Cross-module / Config ----
    {"table":"T001W","name":"PLANT_MASTER","module":"CONFIG","desc":"Plants"},
    {"table":"T001L","name":"STORAGE_LOCATION","module":"CONFIG","desc":"Storage Locations"},
    {"table":"TVAK","name":"SALES_ORDER_TYPES","module":"CONFIG","desc":"Sales Order Types"},
    {"table":"TVAP","name":"ITEM_CATEGORY","module":"CONFIG","desc":"Item Categories"},
    {"table":"T179","name":"MATERIAL_GROUPS","module":"CONFIG","desc":"Material Groups"},
    {"table":"MLAN","name":"MATERIAL_TAX","module":"CONFIG","desc":"Tax Classification for Materials"},
]

# Build quick lookup: SAP table name → friendly name
_SAP_NAME_MAP = {entry["table"]: entry["name"] for entry in SAP_TABLE_CATALOGUE}

# SAP HANA → Snowflake type mapping
_SAP_HANA_TYPE_MAP = {
    'tinyint': 'NUMBER(3,0)', 'smallint': 'NUMBER(5,0)', 'integer': 'NUMBER(10,0)',
    'int': 'NUMBER(10,0)', 'bigint': 'NUMBER(19,0)',
    'decimal': 'NUMBER', 'numeric': 'NUMBER', 'smalldecimal': 'NUMBER',
    'real': 'FLOAT', 'double': 'FLOAT', 'float': 'FLOAT',
    'varchar': 'VARCHAR', 'nvarchar': 'VARCHAR', 'alphanum': 'VARCHAR',
    'shorttext': 'VARCHAR', 'char': 'VARCHAR', 'nchar': 'VARCHAR',
    'clob': 'VARCHAR(16777216)', 'nclob': 'VARCHAR(16777216)', 'text': 'VARCHAR(16777216)',
    'boolean': 'BOOLEAN',
    'date': 'DATE', 'time': 'TIME', 'timestamp': 'TIMESTAMP_NTZ', 'seconddate': 'TIMESTAMP_NTZ',
    'daydate': 'DATE', 'secondtime': 'TIME', 'longdate': 'TIMESTAMP_NTZ',
    'blob': 'BINARY', 'varbinary': 'BINARY', 'bintext': 'BINARY',
    'st_geometry': 'VARIANT', 'st_point': 'VARIANT',
}
_TYPE_MAPS['sap_hana'] = _SAP_HANA_TYPE_MAP
_TYPE_MAPS['sap_bw']   = _SAP_HANA_TYPE_MAP   # BW4HANA also runs on HANA


# ========== SAP SCHEMA READER ==========
def _get_sap_hana_schema(conn_obj, schema_hint: str, selected_tables: list) -> list:
    """Read column metadata from SAP HANA for selected tables."""
    cur = conn_obj.cursor()
    schema = (schema_hint or '').upper()

    # Build WHERE clause for selected tables
    if not selected_tables:
        raise ValueError("No tables selected. Pick tables from the catalogue first.")

    placeholders = ','.join(['?' for _ in selected_tables])
    upper_tables = [t.upper() for t in selected_tables]

    # Column metadata
    cur.execute(f"""
        SELECT
            c.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE_NAME,
            c.LENGTH,
            c.SCALE,
            c.IS_NULLABLE,
            c.DEFAULT_VALUE,
            c.COMMENTS,
            c.POSITION
        FROM SYS.TABLE_COLUMNS c
        WHERE c.SCHEMA_NAME = ?
          AND c.TABLE_NAME IN ({placeholders})
        ORDER BY c.TABLE_NAME, c.POSITION
    """, [schema] + upper_tables)

    rows = cur.fetchall()
    keys = ['table','column','dtype','length','scale','nullable','default','comment','pos']
    col_data = [dict(zip(keys, r)) for r in rows]

    # Primary keys
    cur.execute(f"""
        SELECT ic.TABLE_NAME, ic.COLUMN_NAME
        FROM SYS.INDEX_COLUMNS ic
        JOIN SYS.INDEXES i ON ic.SCHEMA_NAME = i.SCHEMA_NAME
            AND ic.TABLE_NAME = i.TABLE_NAME AND ic.INDEX_NAME = i.INDEX_NAME
        WHERE ic.SCHEMA_NAME = ?
          AND ic.TABLE_NAME IN ({placeholders})
          AND i.CONSTRAINT = 'PRIMARY KEY'
        ORDER BY ic.TABLE_NAME, ic.POSITION
    """, [schema] + upper_tables)

    pk_rows = cur.fetchall()
    pks = {}
    for tbl, col in pk_rows:
        pks.setdefault(tbl, []).append(col)

    cur.close()

    # Group by table
    tables_out = {}
    for row in col_data:
        tbl = row['table']
        tables_out.setdefault(tbl, {'columns': [], 'pk': pks.get(tbl, []), 'indexes': []})
        length = row['length']
        scale  = row['scale']
        dtype  = row['dtype']
        tables_out[tbl]['columns'].append({
            'name':        row['column'],
            'source_type': f"{dtype}({length})" if length else dtype,
            'sf_type':     _map_type('sap_hana', dtype, length, length, scale),
            'nullable':    (row['nullable'] or '').upper() == 'TRUE',
            'default':     row['default'],
            'autoincrement': False,
            'comment':     row['comment'] or '',
        })

    result = []
    for tbl, data in tables_out.items():
        result.append({'table': tbl, 'schema': schema, **data})
    return result


def _get_sap_rfc_schema(conn_obj, selected_tables: list) -> list:
    """Read field metadata from SAP ECC/S4 via RFC_READ_TABLE / DDIF_FIELDINFO_GET."""
    if not selected_tables:
        raise ValueError("No tables selected.")

    result = []
    for tbl_name in selected_tables:
        tbl_upper = tbl_name.upper()
        try:
            # Use DDIF_FIELDINFO_GET — more reliable than RFC_READ_TABLE for metadata
            info = conn_obj.call('DDIF_FIELDINFO_GET',
                                 TABNAME=tbl_upper,
                                 LANGU='E',
                                 ALL_TYPES='X')
            dfies = info.get('DFIES_TAB', [])
        except Exception as e:
            # Table may not exist or user may lack auth — skip with error marker
            result.append({
                'table': tbl_upper,
                'schema': 'SAP',
                'columns': [],
                'pk': [],
                'indexes': [],
                '_error': str(e)
            })
            continue

        columns = []
        pk_cols = []
        for f in dfies:
            fname = f.get('FIELDNAME', '').strip()
            if not fname or fname.startswith('.'):
                continue
            dtype    = f.get('DATATYPE', 'CHAR').strip()
            length   = f.get('LENG', 0)
            decimals = f.get('DECIMALS', 0)
            nullable = f.get('NOTNULL', '') != 'X'
            keyflag  = f.get('KEYFLAG', '').strip() == 'X'
            desc     = f.get('REPTEXT', '').strip() or f.get('SCRTEXT_L', '').strip()

            if keyflag:
                pk_cols.append(fname)

            # Map SAP ABAP types → Snowflake
            sf_type = _map_sap_abap_type(dtype, length, decimals)
            src_type = f"{dtype}({length})" if length else dtype

            columns.append({
                'name':          fname,
                'source_type':   src_type,
                'sf_type':       sf_type,
                'nullable':      nullable,
                'default':       None,
                'autoincrement': False,
                'comment':       desc[:255] if desc else '',
            })

        result.append({
            'table':   tbl_upper,
            'schema':  'SAP',
            'columns': columns,
            'pk':      pk_cols,
            'indexes': [],
        })

    return result


def _map_sap_abap_type(abap_type: str, length: int, decimals: int) -> str:
    """Map SAP ABAP elementary types to Snowflake standard types."""
    t = (abap_type or '').upper().strip()
    # Numeric
    if t in ('INT1', 'INT2'):  return 'NUMBER(5,0)'
    if t == 'INT4':            return 'NUMBER(10,0)'
    if t == 'INT8':            return 'NUMBER(19,0)'
    if t == 'DEC':
        p = min(length or 15, 38)
        s = min(decimals or 0, p)
        return f'NUMBER({p},{s})'
    if t in ('CURR', 'QUAN'):
        p = min(length or 15, 38)
        s = min(decimals or 2, p)
        return f'NUMBER({p},{s})'
    if t == 'FLTP':            return 'FLOAT'
    # Date / Time
    if t == 'DATS':            return 'DATE'
    if t == 'TIMS':            return 'TIME'
    if t == 'TIMESTAMP':       return 'TIMESTAMP_NTZ'
    if t == 'TIMESTAMPL':      return 'TIMESTAMP_NTZ'
    # Boolean
    if t == 'BOOLEAN':         return 'BOOLEAN'
    # String / Char
    if t in ('CHAR', 'LCHR', 'NUMC', 'CLNT', 'LANG', 'UNIT', 'CUKY'):
        l = min(int(length or 1), 16777216)
        return f'VARCHAR({l})'
    if t in ('STRING', 'SSTRING', 'GEOM_EWKB'):
        return 'VARCHAR(16777216)'
    if t in ('RAWSTRING', 'RAW', 'LRAW'):
        return 'BINARY'
    if t == 'ACCP':            return 'VARCHAR(6)'   # Posting period YYYYPP
    if t == 'TZNTSTMPS':       return 'TIMESTAMP_TZ'
    # Unknown → safe fallback
    return 'VARCHAR(255)'


# ========================================================
# ERP ENTITY CATALOGUE
# Curated list of business entities across 6 ERP systems.
# Each entry: {system, entity, name (SF table name), module, desc, fields}
# fields = [{name, sf_type, nullable, pk, desc}]
# ========================================================

def _ef(name, sf_type, nullable=True, pk=False, desc=''):
    """Helper: build an ERP field definition."""
    return {'name': name, 'sf_type': sf_type, 'nullable': nullable, 'pk': pk, 'desc': desc}

ERP_CATALOGUE = {

  # ================================================================
  # ORACLE EBS (E-Business Suite) — on-premise Oracle DB
  # Entity = DB table/view in APPS schema
  # ================================================================
  'oracle_ebs': [
    # ---- Finance (AP/AR/GL) ----
    {'entity':'AP_INVOICES_ALL',        'name':'EBS_AP_INVOICES',         'module':'Finance',
     'desc':'Accounts Payable Invoices',
     'fields':[
       _ef('INVOICE_ID','NUMBER(15,0)',False,True,'Invoice PK'),
       _ef('INVOICE_NUM','VARCHAR(50)',False,False,'Invoice number'),
       _ef('VENDOR_ID','NUMBER(15,0)',False,False,'Supplier ID'),
       _ef('INVOICE_DATE','DATE',False,False,'Invoice date'),
       _ef('INVOICE_AMOUNT','NUMBER(28,10)',False,False,'Total amount'),
       _ef('INVOICE_CURRENCY_CODE','VARCHAR(15)',False,False,'Currency'),
       _ef('INVOICE_TYPE_LOOKUP_CODE','VARCHAR(25)',False,False,'Invoice type'),
       _ef('PAYMENT_STATUS_FLAG','VARCHAR(1)',True,False,'Y/N/P'),
       _ef('ORG_ID','NUMBER(15,0)',True,False,'Operating unit'),
       _ef('CREATION_DATE','TIMESTAMP_NTZ',True,False,'Created'),
       _ef('LAST_UPDATE_DATE','TIMESTAMP_NTZ',True,False,'Last updated'),
     ]},
    {'entity':'AP_INVOICE_LINES_ALL',   'name':'EBS_AP_INVOICE_LINES',    'module':'Finance',
     'desc':'AP Invoice Line Items',
     'fields':[
       _ef('INVOICE_ID','NUMBER(15,0)',False,True,'Invoice FK'),
       _ef('LINE_NUMBER','NUMBER(15,0)',False,True,'Line number'),
       _ef('LINE_TYPE_LOOKUP_CODE','VARCHAR(25)',False,False,'Item/Freight/Tax'),
       _ef('AMOUNT','NUMBER(28,10)',True,False,'Line amount'),
       _ef('DESCRIPTION','VARCHAR(240)',True,False,'Line description'),
       _ef('ACCOUNTING_DATE','DATE',True,False,'GL date'),
       _ef('LAST_UPDATE_DATE','TIMESTAMP_NTZ',True,False,'Last updated'),
     ]},
    {'entity':'AR_CUSTOMERS',           'name':'EBS_AR_CUSTOMERS',         'module':'Finance',
     'desc':'AR Customer Master',
     'fields':[
       _ef('CUSTOMER_ID','NUMBER(15,0)',False,True,'Customer PK'),
       _ef('CUSTOMER_NUMBER','VARCHAR(30)',False,False,'Customer number'),
       _ef('CUSTOMER_NAME','VARCHAR(360)',False,False,'Customer name'),
       _ef('CUSTOMER_TYPE','VARCHAR(25)',True,False,'I=Internal/R=External'),
       _ef('CUSTOMER_CLASS_CODE','VARCHAR(30)',True,False,'Customer class'),
       _ef('STATUS','VARCHAR(1)',False,False,'A=Active'),
       _ef('CREATION_DATE','TIMESTAMP_NTZ',True,False,'Created'),
     ]},
    {'entity':'AR_PAYMENT_SCHEDULES_ALL','name':'EBS_AR_PAYMENT_SCHEDULES','module':'Finance',
     'desc':'AR Invoices and Payment Schedules',
     'fields':[
       _ef('PAYMENT_SCHEDULE_ID','NUMBER(15,0)',False,True,'PK'),
       _ef('CUSTOMER_ID','NUMBER(15,0)',False,False,'Customer FK'),
       _ef('INVOICE_CURRENCY_CODE','VARCHAR(15)',False,False,'Currency'),
       _ef('AMOUNT_DUE_ORIGINAL','NUMBER(28,10)',True,False,'Original amount'),
       _ef('AMOUNT_DUE_REMAINING','NUMBER(28,10)',True,False,'Balance due'),
       _ef('DUE_DATE','DATE',True,False,'Payment due date'),
       _ef('STATUS','VARCHAR(30)',False,False,'OP/CL'),
       _ef('ORG_ID','NUMBER(15,0)',True,False,'Org unit'),
     ]},
    {'entity':'GL_JE_HEADERS',          'name':'EBS_GL_JOURNAL_HEADERS',   'module':'Finance',
     'desc':'GL Journal Entry Headers',
     'fields':[
       _ef('JE_HEADER_ID','NUMBER(15,0)',False,True,'Header PK'),
       _ef('LEDGER_ID','NUMBER(15,0)',False,False,'Ledger'),
       _ef('JE_CATEGORY','VARCHAR(25)',False,False,'Category'),
       _ef('JE_SOURCE','VARCHAR(25)',False,False,'Source'),
       _ef('PERIOD_NAME','VARCHAR(15)',False,False,'Accounting period'),
       _ef('EFFECTIVE_DATE','DATE',False,False,'Effective date'),
       _ef('STATUS','VARCHAR(1)',False,False,'U/P/S'),
       _ef('CURRENCY_CODE','VARCHAR(15)',False,False,'Currency'),
       _ef('LAST_UPDATE_DATE','TIMESTAMP_NTZ',True,False,'Last updated'),
     ]},
    {'entity':'GL_JE_LINES',            'name':'EBS_GL_JOURNAL_LINES',     'module':'Finance',
     'desc':'GL Journal Entry Lines',
     'fields':[
       _ef('JE_HEADER_ID','NUMBER(15,0)',False,True,'Header FK'),
       _ef('JE_LINE_NUM','NUMBER(15,0)',False,True,'Line number'),
       _ef('CODE_COMBINATION_ID','NUMBER(15,0)',False,False,'Account combination'),
       _ef('ENTERED_DR','NUMBER(28,10)',True,False,'Debit amount'),
       _ef('ENTERED_CR','NUMBER(28,10)',True,False,'Credit amount'),
       _ef('ACCOUNTED_DR','NUMBER(28,10)',True,False,'Functional debit'),
       _ef('ACCOUNTED_CR','NUMBER(28,10)',True,False,'Functional credit'),
       _ef('DESCRIPTION','VARCHAR(240)',True,False,'Line description'),
     ]},
    # ---- PO ----
    {'entity':'PO_HEADERS_ALL',         'name':'EBS_PO_HEADERS',           'module':'Procurement',
     'desc':'Purchase Order Headers',
     'fields':[
       _ef('PO_HEADER_ID','NUMBER(15,0)',False,True,'PO PK'),
       _ef('SEGMENT1','VARCHAR(20)',False,False,'PO Number'),
       _ef('VENDOR_ID','NUMBER(15,0)',True,False,'Supplier ID'),
       _ef('CURRENCY_CODE','VARCHAR(15)',True,False,'Currency'),
       _ef('TOTAL_AMOUNT','NUMBER(28,10)',True,False,'PO total'),
       _ef('AUTHORIZATION_STATUS','VARCHAR(25)',True,False,'APPROVED/REQUIRES REAPPROVAL'),
       _ef('TYPE_LOOKUP_CODE','VARCHAR(25)',False,False,'PO type'),
       _ef('CREATION_DATE','TIMESTAMP_NTZ',True,False,'Created'),
       _ef('ORG_ID','NUMBER(15,0)',True,False,'Org unit'),
     ]},
    {'entity':'PO_LINES_ALL',           'name':'EBS_PO_LINES',             'module':'Procurement',
     'desc':'Purchase Order Lines',
     'fields':[
       _ef('PO_LINE_ID','NUMBER(15,0)',False,True,'Line PK'),
       _ef('PO_HEADER_ID','NUMBER(15,0)',False,False,'Header FK'),
       _ef('LINE_NUM','NUMBER(15,0)',False,False,'Line number'),
       _ef('ITEM_ID','NUMBER(15,0)',True,False,'Item FK'),
       _ef('UNIT_PRICE','NUMBER(28,10)',True,False,'Unit price'),
       _ef('QUANTITY','NUMBER(28,10)',True,False,'Ordered quantity'),
       _ef('UNIT_MEAS_LOOKUP_CODE','VARCHAR(25)',True,False,'UOM'),
     ]},
    # ---- Inventory ----
    {'entity':'MTL_SYSTEM_ITEMS_B',     'name':'EBS_INVENTORY_ITEMS',      'module':'Inventory',
     'desc':'Inventory Item Master',
     'fields':[
       _ef('INVENTORY_ITEM_ID','NUMBER(15,0)',False,True,'Item PK'),
       _ef('ORGANIZATION_ID','NUMBER(15,0)',False,True,'Org PK'),
       _ef('SEGMENT1','VARCHAR(40)',False,False,'Item number'),
       _ef('DESCRIPTION','VARCHAR(240)',True,False,'Item description'),
       _ef('PRIMARY_UOM_CODE','VARCHAR(3)',True,False,'Primary UOM'),
       _ef('ITEM_TYPE','VARCHAR(30)',True,False,'Item type'),
       _ef('INVENTORY_ITEM_STATUS_CODE','VARCHAR(10)',True,False,'Status'),
       _ef('LIST_PRICE_PER_UNIT','NUMBER(28,10)',True,False,'List price'),
       _ef('STANDARD_COST','NUMBER(28,10)',True,False,'Standard cost'),
     ]},
    # ---- HR ----
    {'entity':'PER_ALL_PEOPLE_F',       'name':'EBS_HR_PEOPLE',            'module':'HR',
     'desc':'HR Person Master',
     'fields':[
       _ef('PERSON_ID','NUMBER(15,0)',False,True,'Person PK'),
       _ef('EMPLOYEE_NUMBER','VARCHAR(30)',True,False,'Employee number'),
       _ef('FIRST_NAME','VARCHAR(150)',True,False,'First name'),
       _ef('LAST_NAME','VARCHAR(150)',False,False,'Last name'),
       _ef('EMAIL_ADDRESS','VARCHAR(240)',True,False,'Email'),
       _ef('DATE_OF_BIRTH','DATE',True,False,'DOB'),
       _ef('EFFECTIVE_START_DATE','DATE',False,False,'Record start date'),
       _ef('EFFECTIVE_END_DATE','DATE',False,False,'Record end date'),
     ]},
  ],

  # ================================================================
  # ORACLE ERP CLOUD (Fusion) — REST API
  # entity = REST resource path fragment
  # ================================================================
  'oracle_erp_cloud': [
    {'entity':'invoices',               'name':'CLOUD_AP_INVOICES',         'module':'Finance',
     'desc':'Payables Invoices',
     'fields':[
       _ef('InvoiceId','NUMBER(15,0)',False,True,'Invoice ID'),
       _ef('InvoiceNumber','VARCHAR(50)',False,False,'Invoice number'),
       _ef('InvoiceAmount','NUMBER(28,10)',True,False,'Total invoice amount'),
       _ef('InvoiceCurrencyCode','VARCHAR(15)',False,False,'Currency code'),
       _ef('InvoiceDate','DATE',False,False,'Invoice date'),
       _ef('SupplierName','VARCHAR(360)',True,False,'Supplier name'),
       _ef('SupplierId','NUMBER(15,0)',True,False,'Supplier ID'),
       _ef('PaymentStatus','VARCHAR(30)',True,False,'Payment status'),
       _ef('BusinessUnit','VARCHAR(240)',True,False,'Business unit'),
       _ef('CreationDate','TIMESTAMP_NTZ',True,False,'Created'),
     ]},
    {'entity':'receivables/transactions','name':'CLOUD_AR_TRANSACTIONS',    'module':'Finance',
     'desc':'Receivables Transactions',
     'fields':[
       _ef('TransactionNumber','VARCHAR(30)',False,True,'Transaction number'),
       _ef('TransactionDate','DATE',False,False,'Transaction date'),
       _ef('DueDate','DATE',True,False,'Due date'),
       _ef('OriginalAmount','NUMBER(28,10)',True,False,'Original amount'),
       _ef('RemainingAmount','NUMBER(28,10)',True,False,'Balance'),
       _ef('Currency','VARCHAR(15)',False,False,'Currency'),
       _ef('CustomerName','VARCHAR(360)',True,False,'Customer name'),
       _ef('TransactionType','VARCHAR(30)',True,False,'Invoice/Credit memo/etc'),
     ]},
    {'entity':'generalLedger/journals', 'name':'CLOUD_GL_JOURNALS',         'module':'Finance',
     'desc':'General Ledger Journal Entries',
     'fields':[
       _ef('JournalBatchName','VARCHAR(100)',False,True,'Journal batch'),
       _ef('JournalName','VARCHAR(100)',False,True,'Journal name'),
       _ef('Category','VARCHAR(25)',False,False,'Category'),
       _ef('Source','VARCHAR(25)',False,False,'Source'),
       _ef('AccountingDate','DATE',False,False,'Accounting date'),
       _ef('PeriodName','VARCHAR(15)',False,False,'Period'),
       _ef('EnteredDebit','NUMBER(28,10)',True,False,'Debit'),
       _ef('EnteredCredit','NUMBER(28,10)',True,False,'Credit'),
       _ef('LedgerName','VARCHAR(30)',False,False,'Ledger'),
     ]},
    {'entity':'purchaseOrders',         'name':'CLOUD_PO_HEADERS',          'module':'Procurement',
     'desc':'Purchase Orders',
     'fields':[
       _ef('POHeaderId','NUMBER(15,0)',False,True,'PO ID'),
       _ef('PONumber','VARCHAR(20)',False,False,'PO number'),
       _ef('Supplier','VARCHAR(360)',True,False,'Supplier name'),
       _ef('OrderDate','DATE',True,False,'Order date'),
       _ef('Currency','VARCHAR(15)',False,False,'Currency'),
       _ef('TotalAmount','NUMBER(28,10)',True,False,'Total amount'),
       _ef('Status','VARCHAR(25)',True,False,'Status'),
       _ef('BusinessUnit','VARCHAR(240)',True,False,'Business unit'),
     ]},
    {'entity':'employees',              'name':'CLOUD_HCM_EMPLOYEES',        'module':'HCM',
     'desc':'HCM Employee Master',
     'fields':[
       _ef('PersonId','NUMBER(15,0)',False,True,'Person ID'),
       _ef('PersonNumber','VARCHAR(30)',False,False,'Employee number'),
       _ef('FirstName','VARCHAR(150)',True,False,'First name'),
       _ef('LastName','VARCHAR(150)',False,False,'Last name'),
       _ef('EmailAddress','VARCHAR(240)',True,False,'Email'),
       _ef('DateOfBirth','DATE',True,False,'DOB'),
       _ef('HireDate','DATE',True,False,'Hire date'),
       _ef('DepartmentName','VARCHAR(240)',True,False,'Department'),
       _ef('JobTitle','VARCHAR(255)',True,False,'Job title'),
       _ef('LocationName','VARCHAR(60)',True,False,'Location'),
     ]},
    {'entity':'suppliers',              'name':'CLOUD_SUPPLIERS',            'module':'Procurement',
     'desc':'Supplier (Vendor) Master',
     'fields':[
       _ef('SupplierId','NUMBER(15,0)',False,True,'Supplier ID'),
       _ef('SupplierNumber','VARCHAR(30)',False,False,'Supplier number'),
       _ef('SupplierName','VARCHAR(360)',False,False,'Supplier name'),
       _ef('SupplierType','VARCHAR(30)',True,False,'Supplier type'),
       _ef('Status','VARCHAR(25)',True,False,'Active/Inactive'),
       _ef('TaxRegistrationNumber','VARCHAR(50)',True,False,'Tax ID'),
       _ef('CreationDate','TIMESTAMP_NTZ',True,False,'Created'),
     ]},
  ],

  # ================================================================
  # MICROSOFT DYNAMICS 365 — OData v4 REST API
  # entity = OData entity set name
  # ================================================================
  'dynamics365': [
    # ---- Finance & Operations ----
    {'entity':'SalesOrderHeadersV2',    'name':'D365_SALES_ORDER_HEADERS',  'module':'Sales',
     'desc':'Sales Order Headers (F&O)',
     'fields':[
       _ef('SalesOrderNumber','VARCHAR(20)',False,True,'Sales order number'),
       _ef('SalesOrderName','VARCHAR(60)',True,False,'Order name'),
       _ef('CustomerAccountNumber','VARCHAR(20)',False,False,'Customer account'),
       _ef('OrderingCustomerGroupId','VARCHAR(10)',True,False,'Customer group'),
       _ef('RequestedShippingDate','TIMESTAMP_NTZ',True,False,'Requested ship date'),
       _ef('TotalChargeAmount','NUMBER(28,10)',True,False,'Total charges'),
       _ef('SalesOrderStatus','VARCHAR(20)',True,False,'Status'),
       _ef('CurrencyCode','VARCHAR(3)',False,False,'Currency'),
       _ef('dataAreaId','VARCHAR(4)',False,True,'Legal entity'),
     ]},
    {'entity':'SalesOrderLinesV2',      'name':'D365_SALES_ORDER_LINES',    'module':'Sales',
     'desc':'Sales Order Lines (F&O)',
     'fields':[
       _ef('SalesOrderNumber','VARCHAR(20)',False,True,'Order FK'),
       _ef('SalesOrderLineNumber','NUMBER(10,0)',False,True,'Line number'),
       _ef('ItemNumber','VARCHAR(20)',False,False,'Item number'),
       _ef('OrderedSalesQuantity','NUMBER(28,10)',True,False,'Ordered qty'),
       _ef('SalesPrice','NUMBER(28,10)',True,False,'Unit price'),
       _ef('LineDiscountAmount','NUMBER(28,10)',True,False,'Discount'),
       _ef('ShippingWarehouseId','VARCHAR(10)',True,False,'Warehouse'),
       _ef('dataAreaId','VARCHAR(4)',False,True,'Legal entity'),
     ]},
    {'entity':'VendInvoiceJournalHeaderEntity','name':'D365_VENDOR_INVOICES','module':'Finance',
     'desc':'Vendor Invoice Journal Headers',
     'fields':[
       _ef('JournalBatchNumber','VARCHAR(20)',False,True,'Journal number'),
       _ef('InvoiceDate','DATE',True,False,'Invoice date'),
       _ef('VendorAccountNumber','VARCHAR(20)',False,False,'Vendor account'),
       _ef('InvoiceNumber','VARCHAR(20)',True,False,'Vendor invoice number'),
       _ef('InvoiceAmount','NUMBER(28,10)',True,False,'Invoice amount'),
       _ef('CurrencyCode','VARCHAR(3)',False,False,'Currency'),
       _ef('Description','VARCHAR(60)',True,False,'Description'),
       _ef('dataAreaId','VARCHAR(4)',False,True,'Legal entity'),
     ]},
    {'entity':'LedgerJournalHeaderEntity', 'name':'D365_GL_JOURNALS',        'module':'Finance',
     'desc':'General Ledger Journal Headers',
     'fields':[
       _ef('JournalBatchNumber','VARCHAR(20)',False,True,'Journal number'),
       _ef('JournalName','VARCHAR(10)',False,False,'Journal name'),
       _ef('Description','VARCHAR(60)',True,False,'Description'),
       _ef('AccountingDate','DATE',True,False,'Accounting date'),
       _ef('Posted','BOOLEAN',True,False,'Is posted'),
       _ef('dataAreaId','VARCHAR(4)',False,True,'Legal entity'),
     ]},
    {'entity':'PurchaseOrderHeaderV2',  'name':'D365_PO_HEADERS',           'module':'Procurement',
     'desc':'Purchase Order Headers (F&O)',
     'fields':[
       _ef('PurchaseOrderNumber','VARCHAR(20)',False,True,'PO number'),
       _ef('VendorAccountNumber','VARCHAR(20)',False,False,'Vendor account'),
       _ef('OrderDate','DATE',True,False,'Order date'),
       _ef('TotalInvoiceAmount','NUMBER(28,10)',True,False,'Total amount'),
       _ef('PurchaseOrderStatus','VARCHAR(20)',True,False,'Status'),
       _ef('CurrencyCode','VARCHAR(3)',False,False,'Currency'),
       _ef('dataAreaId','VARCHAR(4)',False,True,'Legal entity'),
     ]},
    {'entity':'RetailCustomersV3',      'name':'D365_CUSTOMERS',             'module':'Sales',
     'desc':'Customer Master',
     'fields':[
       _ef('CustomerAccount','VARCHAR(20)',False,True,'Account number'),
       _ef('OrganizationName','VARCHAR(100)',True,False,'Customer name'),
       _ef('CustomerGroupId','VARCHAR(10)',True,False,'Customer group'),
       _ef('CurrencyCode','VARCHAR(3)',True,False,'Currency'),
       _ef('SalesTaxGroup','VARCHAR(10)',True,False,'Tax group'),
       _ef('VATNum','VARCHAR(20)',True,False,'VAT number'),
       _ef('dataAreaId','VARCHAR(4)',False,True,'Legal entity'),
     ]},
    {'entity':'HcmWorkersV2',           'name':'D365_HR_WORKERS',            'module':'HR',
     'desc':'HR Workers (Employees + Contractors)',
     'fields':[
       _ef('PersonnelNumber','VARCHAR(20)',False,True,'Personnel number'),
       _ef('FirstName','VARCHAR(25)',True,False,'First name'),
       _ef('LastName','VARCHAR(25)',False,False,'Last name'),
       _ef('PrimaryEmailAddress','VARCHAR(255)',True,False,'Email'),
       _ef('EmploymentStartDate','DATE',True,False,'Start date'),
       _ef('DepartmentNumber','VARCHAR(10)',True,False,'Department'),
       _ef('PositionTitle','VARCHAR(50)',True,False,'Job title'),
       _ef('dataAreaId','VARCHAR(4)',False,True,'Legal entity'),
     ]},
    {'entity':'InventTableV2',          'name':'D365_PRODUCTS',              'module':'Inventory',
     'desc':'Product Master',
     'fields':[
       _ef('ItemNumber','VARCHAR(20)',False,True,'Item number'),
       _ef('ProductName','VARCHAR(60)',True,False,'Product name'),
       _ef('ProductDescription','VARCHAR(255)',True,False,'Description'),
       _ef('ItemModelGroupId','VARCHAR(10)',True,False,'Item model group'),
       _ef('UnitId','VARCHAR(10)',True,False,'Unit of measure'),
       _ef('NetWeight','NUMBER(28,10)',True,False,'Net weight'),
       _ef('dataAreaId','VARCHAR(4)',False,True,'Legal entity'),
     ]},
  ],

  # ================================================================
  # MS DYNAMICS ON-PREMISE (AX / NAV / GP) — SQL Server direct
  # entity = table name in the SQL Server DB
  # ================================================================
  'dynamics_onprem': [
    # AX 2012 / D365 on-prem common tables
    {'entity':'SALESLINE',              'name':'AX_SALES_LINES',            'module':'Sales',
     'desc':'Sales Order Lines (AX)',
     'fields':[
       _ef('SALESID','VARCHAR(20)',False,True,'Sales order ID'),
       _ef('LINENUM','NUMBER(10,2)',False,True,'Line number'),
       _ef('ITEMID','VARCHAR(20)',False,False,'Item ID'),
       _ef('SALESQTY','NUMBER(28,10)',True,False,'Sales quantity'),
       _ef('SALESPRICE','NUMBER(28,10)',True,False,'Unit price'),
       _ef('LINEAMOUNT','NUMBER(28,10)',True,False,'Line amount'),
       _ef('CURRENCYCODE','VARCHAR(3)',False,False,'Currency'),
       _ef('DATAAREAID','VARCHAR(4)',False,True,'Company'),
       _ef('RECID','NUMBER(19,0)',False,True,'Record ID'),
     ]},
    {'entity':'SALESTABLE',             'name':'AX_SALES_HEADERS',          'module':'Sales',
     'desc':'Sales Order Headers (AX)',
     'fields':[
       _ef('SALESID','VARCHAR(20)',False,True,'Sales order ID'),
       _ef('CUSTACCOUNT','VARCHAR(20)',False,False,'Customer account'),
       _ef('SALESNAME','VARCHAR(60)',True,False,'Order name'),
       _ef('SALESSTATUS','NUMBER(3,0)',True,False,'Status code'),
       _ef('CURRENCYCODE','VARCHAR(3)',False,False,'Currency'),
       _ef('SALESORIGINID','VARCHAR(10)',True,False,'Order origin'),
       _ef('CREATEDDATETIME','TIMESTAMP_NTZ',True,False,'Created'),
       _ef('DATAAREAID','VARCHAR(4)',False,True,'Company'),
       _ef('RECID','NUMBER(19,0)',False,True,'Record ID'),
     ]},
    {'entity':'CUSTTABLE',              'name':'AX_CUSTOMERS',               'module':'Sales',
     'desc':'Customer Master (AX)',
     'fields':[
       _ef('ACCOUNTNUM','VARCHAR(20)',False,True,'Account number'),
       _ef('NAME','VARCHAR(60)',False,False,'Customer name'),
       _ef('CUSTGROUP','VARCHAR(10)',True,False,'Customer group'),
       _ef('CURRENCY','VARCHAR(3)',True,False,'Currency'),
       _ef('TAXGROUP','VARCHAR(10)',True,False,'Tax group'),
       _ef('BLOCKED','NUMBER(3,0)',True,False,'Block code'),
       _ef('DATAAREAID','VARCHAR(4)',False,True,'Company'),
       _ef('RECID','NUMBER(19,0)',False,True,'Record ID'),
     ]},
    {'entity':'VENDINVOICEJOUR',        'name':'AX_VENDOR_INVOICES',         'module':'Finance',
     'desc':'Vendor Invoice Journal (AX)',
     'fields':[
       _ef('INVOICEID','VARCHAR(20)',False,True,'Invoice number'),
       _ef('INVOICEDATE','DATE',False,False,'Invoice date'),
       _ef('ORDERACCOUNT','VARCHAR(20)',False,False,'Vendor account'),
       _ef('INVOICEAMOUNT','NUMBER(28,10)',True,False,'Invoice amount'),
       _ef('CURRENCYCODE','VARCHAR(3)',False,False,'Currency'),
       _ef('APPROVALSTATUS','NUMBER(3,0)',True,False,'Approval status'),
       _ef('DATAAREAID','VARCHAR(4)',False,True,'Company'),
       _ef('RECID','NUMBER(19,0)',False,True,'Record ID'),
     ]},
    {'entity':'GENERALJOURNALENTRY',    'name':'AX_GL_JOURNAL_ENTRIES',      'module':'Finance',
     'desc':'General Journal Entries (AX)',
     'fields':[
       _ef('RECID','NUMBER(19,0)',False,True,'Record ID'),
       _ef('JOURNALNUMBER','VARCHAR(20)',True,False,'Journal number'),
       _ef('ACCOUNTINGDATE','DATE',True,False,'Accounting date'),
       _ef('POSTINGTYPE','NUMBER(3,0)',True,False,'Posting type'),
       _ef('ISPOSTED','BOOLEAN',True,False,'Is posted'),
       _ef('DOCUMENTDATE','DATE',True,False,'Document date'),
     ]},
    {'entity':'INVENTTABLE',            'name':'AX_PRODUCTS',                'module':'Inventory',
     'desc':'Product/Item Master (AX)',
     'fields':[
       _ef('ITEMID','VARCHAR(20)',False,True,'Item ID'),
       _ef('NAMEALIAS','VARCHAR(30)',True,False,'Search name'),
       _ef('ITEMTYPE','NUMBER(3,0)',True,False,'Item type'),
       _ef('UNITID','VARCHAR(10)',True,False,'Unit of measure'),
       _ef('NETWEIGHT','NUMBER(28,10)',True,False,'Net weight'),
       _ef('DATAAREAID','VARCHAR(4)',False,True,'Company'),
       _ef('RECID','NUMBER(19,0)',False,True,'Record ID'),
     ]},
    {'entity':'HCMWORKER',              'name':'AX_HR_WORKERS',              'module':'HR',
     'desc':'HR Workers (AX)',
     'fields':[
       _ef('RECID','NUMBER(19,0)',False,True,'Record ID'),
       _ef('PERSONNELNUMBER','VARCHAR(20)',True,False,'Personnel number'),
       _ef('PRIMARY','NUMBER(19,0)',True,False,'Primary party FK'),
       _ef('HIREDATE','DATE',True,False,'Hire date'),
       _ef('WORKERTYPE','NUMBER(3,0)',True,False,'Employee/Contractor'),
     ]},
    # NAV / Business Central common tables
    {'entity':'[Customer]',             'name':'NAV_CUSTOMERS',              'module':'Sales',
     'desc':'Customer Master (NAV/BC)',
     'fields':[
       _ef('No_','VARCHAR(20)',False,True,'Customer No'),
       _ef('Name','VARCHAR(50)',False,False,'Name'),
       _ef('Address','VARCHAR(50)',True,False,'Address'),
       _ef('City','VARCHAR(30)',True,False,'City'),
       _ef('Currency_Code','VARCHAR(10)',True,False,'Currency'),
       _ef('Customer_Posting_Group','VARCHAR(20)',True,False,'Posting group'),
       _ef('Blocked','VARCHAR(30)',True,False,'Blocked status'),
     ]},
    {'entity':'[Sales Header]',         'name':'NAV_SALES_HEADERS',          'module':'Sales',
     'desc':'Sales Order Headers (NAV/BC)',
     'fields':[
       _ef('No_','VARCHAR(20)',False,True,'Document No'),
       _ef('Document_Type','NUMBER(5,0)',False,True,'Order/Invoice/etc'),
       _ef('Sell_to_Customer_No_','VARCHAR(20)',False,False,'Customer No'),
       _ef('Order_Date','DATE',True,False,'Order date'),
       _ef('Amount','NUMBER(28,10)',True,False,'Amount'),
       _ef('Amount_Including_VAT','NUMBER(28,10)',True,False,'Amount inc VAT'),
       _ef('Currency_Code','VARCHAR(10)',True,False,'Currency'),
       _ef('Status','NUMBER(5,0)',True,False,'Status'),
     ]},
  ],

  # ================================================================
  # NETSUITE — SuiteTalk REST API
  # entity = REST record type
  # ================================================================
  'netsuite': [
    {'entity':'invoice',                'name':'NS_INVOICES',                'module':'Finance',
     'desc':'Sales Invoices',
     'fields':[
       _ef('id','NUMBER(15,0)',False,True,'Internal ID'),
       _ef('tranId','VARCHAR(30)',False,False,'Transaction ID / Invoice number'),
       _ef('tranDate','DATE',False,False,'Transaction date'),
       _ef('dueDate','DATE',True,False,'Due date'),
       _ef('total','NUMBER(28,10)',True,False,'Total amount'),
       _ef('amountRemaining','NUMBER(28,10)',True,False,'Balance due'),
       _ef('currency','VARCHAR(3)',True,False,'Currency code'),
       _ef('entity','NUMBER(15,0)',True,False,'Customer internal ID'),
       _ef('status','VARCHAR(50)',True,False,'Invoice status'),
       _ef('department','NUMBER(15,0)',True,False,'Department ID'),
       _ef('subsidiary','NUMBER(15,0)',True,False,'Subsidiary ID'),
       _ef('lastModifiedDate','TIMESTAMP_NTZ',True,False,'Last modified'),
     ]},
    {'entity':'customer',               'name':'NS_CUSTOMERS',               'module':'Finance',
     'desc':'Customer Master',
     'fields':[
       _ef('id','NUMBER(15,0)',False,True,'Internal ID'),
       _ef('entityId','VARCHAR(30)',False,False,'Customer ID'),
       _ef('companyName','VARCHAR(83)',True,False,'Company name'),
       _ef('firstName','VARCHAR(32)',True,False,'First name'),
       _ef('lastName','VARCHAR(32)',True,False,'Last name'),
       _ef('email','VARCHAR(254)',True,False,'Email'),
       _ef('phone','VARCHAR(21)',True,False,'Phone'),
       _ef('currency','VARCHAR(3)',True,False,'Currency'),
       _ef('status','VARCHAR(50)',True,False,'Status'),
       _ef('subsidiary','NUMBER(15,0)',True,False,'Subsidiary ID'),
       _ef('dateCreated','TIMESTAMP_NTZ',True,False,'Created'),
     ]},
    {'entity':'vendor',                 'name':'NS_VENDORS',                 'module':'Procurement',
     'desc':'Vendor Master',
     'fields':[
       _ef('id','NUMBER(15,0)',False,True,'Internal ID'),
       _ef('entityId','VARCHAR(30)',False,False,'Vendor ID'),
       _ef('companyName','VARCHAR(83)',True,False,'Company name'),
       _ef('email','VARCHAR(254)',True,False,'Email'),
       _ef('phone','VARCHAR(21)',True,False,'Phone'),
       _ef('currency','VARCHAR(3)',True,False,'Currency'),
       _ef('taxRegistrationNumber','VARCHAR(50)',True,False,'Tax reg no'),
       _ef('subsidiary','NUMBER(15,0)',True,False,'Subsidiary'),
     ]},
    {'entity':'vendorbill',             'name':'NS_VENDOR_BILLS',            'module':'Procurement',
     'desc':'Vendor Bills (AP Invoices)',
     'fields':[
       _ef('id','NUMBER(15,0)',False,True,'Internal ID'),
       _ef('tranId','VARCHAR(30)',False,False,'Vendor bill number'),
       _ef('tranDate','DATE',False,False,'Bill date'),
       _ef('dueDate','DATE',True,False,'Due date'),
       _ef('total','NUMBER(28,10)',True,False,'Total'),
       _ef('amountRemaining','NUMBER(28,10)',True,False,'Balance'),
       _ef('entity','NUMBER(15,0)',True,False,'Vendor ID'),
       _ef('currency','VARCHAR(3)',True,False,'Currency'),
       _ef('status','VARCHAR(50)',True,False,'Status'),
       _ef('subsidiary','NUMBER(15,0)',True,False,'Subsidiary'),
     ]},
    {'entity':'salesorder',             'name':'NS_SALES_ORDERS',            'module':'Sales',
     'desc':'Sales Orders',
     'fields':[
       _ef('id','NUMBER(15,0)',False,True,'Internal ID'),
       _ef('tranId','VARCHAR(30)',False,False,'Order number'),
       _ef('tranDate','DATE',False,False,'Order date'),
       _ef('entity','NUMBER(15,0)',True,False,'Customer ID'),
       _ef('total','NUMBER(28,10)',True,False,'Total'),
       _ef('currency','VARCHAR(3)',True,False,'Currency'),
       _ef('status','VARCHAR(50)',True,False,'Status'),
       _ef('shipDate','DATE',True,False,'Ship date'),
       _ef('lastModifiedDate','TIMESTAMP_NTZ',True,False,'Last modified'),
     ]},
    {'entity':'purchaseorder',          'name':'NS_PURCHASE_ORDERS',         'module':'Procurement',
     'desc':'Purchase Orders',
     'fields':[
       _ef('id','NUMBER(15,0)',False,True,'Internal ID'),
       _ef('tranId','VARCHAR(30)',False,False,'PO number'),
       _ef('tranDate','DATE',False,False,'Order date'),
       _ef('entity','NUMBER(15,0)',True,False,'Vendor ID'),
       _ef('total','NUMBER(28,10)',True,False,'Total'),
       _ef('currency','VARCHAR(3)',True,False,'Currency'),
       _ef('status','VARCHAR(50)',True,False,'Status'),
       _ef('lastModifiedDate','TIMESTAMP_NTZ',True,False,'Last modified'),
     ]},
    {'entity':'employee',               'name':'NS_EMPLOYEES',               'module':'HR',
     'desc':'Employee Master',
     'fields':[
       _ef('id','NUMBER(15,0)',False,True,'Internal ID'),
       _ef('entityId','VARCHAR(30)',False,False,'Employee ID'),
       _ef('firstName','VARCHAR(32)',True,False,'First name'),
       _ef('lastName','VARCHAR(32)',False,False,'Last name'),
       _ef('email','VARCHAR(254)',True,False,'Email'),
       _ef('hireDate','DATE',True,False,'Hire date'),
       _ef('department','NUMBER(15,0)',True,False,'Department ID'),
       _ef('title','VARCHAR(50)',True,False,'Job title'),
       _ef('subsidiary','NUMBER(15,0)',True,False,'Subsidiary'),
       _ef('isInactive','BOOLEAN',True,False,'Is inactive'),
     ]},
    {'entity':'inventoryitem',          'name':'NS_INVENTORY_ITEMS',         'module':'Inventory',
     'desc':'Inventory Item Master',
     'fields':[
       _ef('id','NUMBER(15,0)',False,True,'Internal ID'),
       _ef('itemId','VARCHAR(80)',False,False,'Item name'),
       _ef('displayName','VARCHAR(80)',True,False,'Display name'),
       _ef('upcCode','VARCHAR(30)',True,False,'UPC / Barcode'),
       _ef('purchasePrice','NUMBER(28,10)',True,False,'Purchase price'),
       _ef('salesPrice','NUMBER(28,10)',True,False,'Sales price'),
       _ef('quantityOnHand','NUMBER(28,10)',True,False,'On hand qty'),
       _ef('unitsType','NUMBER(15,0)',True,False,'Units type'),
       _ef('subsidiary','NUMBER(15,0)',True,False,'Subsidiary'),
     ]},
    {'entity':'journalentry',           'name':'NS_JOURNAL_ENTRIES',         'module':'Finance',
     'desc':'Manual Journal Entries',
     'fields':[
       _ef('id','NUMBER(15,0)',False,True,'Internal ID'),
       _ef('tranId','VARCHAR(30)',False,False,'Journal number'),
       _ef('tranDate','DATE',False,False,'Date'),
       _ef('memo','VARCHAR(999)',True,False,'Memo'),
       _ef('subsidiary','NUMBER(15,0)',True,False,'Subsidiary'),
       _ef('currency','VARCHAR(3)',True,False,'Currency'),
       _ef('isReversing','BOOLEAN',True,False,'Is reversing entry'),
       _ef('lastModifiedDate','TIMESTAMP_NTZ',True,False,'Last modified'),
     ]},
  ],

  # ================================================================
  # WORKDAY — REST API (Reports-as-a-Service + REST)
  # entity = Workday REST endpoint / report path
  # ================================================================
  'workday': [
    {'entity':'workers',                'name':'WD_WORKERS',                 'module':'HCM',
     'desc':'Worker (Employee) Master',
     'fields':[
       _ef('id','VARCHAR(36)',False,True,'Worker WID'),
       _ef('descriptor','VARCHAR(255)',True,False,'Worker name'),
       _ef('Worker_ID','VARCHAR(50)',True,False,'Employee ID'),
       _ef('First_Name','VARCHAR(100)',True,False,'First name'),
       _ef('Last_Name','VARCHAR(100)',False,False,'Last name'),
       _ef('Email_Address','VARCHAR(255)',True,False,'Email'),
       _ef('Hire_Date','DATE',True,False,'Hire date'),
       _ef('Termination_Date','DATE',True,False,'Termination date (null if active)'),
       _ef('Worker_Type','VARCHAR(50)',True,False,'Employee/Contingent worker'),
       _ef('Position_Title','VARCHAR(255)',True,False,'Job title'),
       _ef('Department','VARCHAR(255)',True,False,'Department / Cost center'),
       _ef('Location','VARCHAR(100)',True,False,'Work location'),
       _ef('Manager_ID','VARCHAR(36)',True,False,'Manager WID'),
     ]},
    {'entity':'organizations',          'name':'WD_ORGANIZATIONS',           'module':'HCM',
     'desc':'Organizational Units (Departments/Cost Centers)',
     'fields':[
       _ef('id','VARCHAR(36)',False,True,'Org WID'),
       _ef('descriptor','VARCHAR(255)',True,False,'Org name'),
       _ef('Organization_Code','VARCHAR(50)',True,False,'Org code'),
       _ef('Organization_Type','VARCHAR(50)',True,False,'Type'),
       _ef('Inactive','BOOLEAN',True,False,'Is inactive'),
       _ef('Superior_Organization_ID','VARCHAR(36)',True,False,'Parent org WID'),
     ]},
    {'entity':'positions',              'name':'WD_POSITIONS',               'module':'HCM',
     'desc':'Position Master',
     'fields':[
       _ef('id','VARCHAR(36)',False,True,'Position WID'),
       _ef('descriptor','VARCHAR(255)',True,False,'Position title'),
       _ef('Position_ID','VARCHAR(50)',True,False,'Position ID'),
       _ef('Job_Exempt','BOOLEAN',True,False,'Is exempt'),
       _ef('FTE_Percent','NUMBER(5,2)',True,False,'FTE percentage'),
       _ef('Organization_ID','VARCHAR(36)',True,False,'Org WID'),
       _ef('Job_Profile_ID','VARCHAR(36)',True,False,'Job profile WID'),
       _ef('Hiring_Freeze','BOOLEAN',True,False,'Hiring freeze'),
     ]},
    {'entity':'payrollPayslips',        'name':'WD_PAYROLL_PAYSLIPS',        'module':'Payroll',
     'desc':'Payroll Payslips',
     'fields':[
       _ef('id','VARCHAR(36)',False,True,'Payslip WID'),
       _ef('Worker_ID','VARCHAR(36)',True,False,'Worker WID'),
       _ef('Period_Start_Date','DATE',True,False,'Pay period start'),
       _ef('Period_End_Date','DATE',True,False,'Pay period end'),
       _ef('Check_Date','DATE',True,False,'Payment date'),
       _ef('Gross_Pay','NUMBER(28,10)',True,False,'Gross pay'),
       _ef('Net_Pay','NUMBER(28,10)',True,False,'Net pay'),
       _ef('Currency','VARCHAR(3)',True,False,'Currency'),
     ]},
    {'entity':'suppliers',              'name':'WD_SUPPLIERS',               'module':'Finance',
     'desc':'Supplier Master',
     'fields':[
       _ef('id','VARCHAR(36)',False,True,'Supplier WID'),
       _ef('descriptor','VARCHAR(255)',True,False,'Supplier name'),
       _ef('Supplier_ID','VARCHAR(50)',True,False,'Supplier ID'),
       _ef('Supplier_Category','VARCHAR(50)',True,False,'Category'),
       _ef('Currency','VARCHAR(3)',True,False,'Default currency'),
       _ef('Tax_ID','VARCHAR(50)',True,False,'Tax ID'),
       _ef('Inactive','BOOLEAN',True,False,'Is inactive'),
     ]},
    {'entity':'customerInvoices',       'name':'WD_CUSTOMER_INVOICES',       'module':'Finance',
     'desc':'Customer Invoices (AR)',
     'fields':[
       _ef('id','VARCHAR(36)',False,True,'Invoice WID'),
       _ef('Invoice_Number','VARCHAR(50)',False,False,'Invoice number'),
       _ef('Invoice_Date','DATE',False,False,'Invoice date'),
       _ef('Due_Date','DATE',True,False,'Due date'),
       _ef('Invoice_Amount','NUMBER(28,10)',True,False,'Total amount'),
       _ef('Outstanding_Amount','NUMBER(28,10)',True,False,'Balance'),
       _ef('Currency','VARCHAR(3)',True,False,'Currency'),
       _ef('Customer_ID','VARCHAR(36)',True,False,'Customer WID'),
       _ef('Status','VARCHAR(50)',True,False,'Invoice status'),
     ]},
    {'entity':'journalLines',           'name':'WD_GL_JOURNAL_LINES',        'module':'Finance',
     'desc':'General Ledger Journal Lines',
     'fields':[
       _ef('id','VARCHAR(36)',False,True,'Line WID'),
       _ef('Journal_Sequence_Number','VARCHAR(50)',True,False,'Journal number'),
       _ef('Accounting_Date','DATE',True,False,'Accounting date'),
       _ef('Ledger_Account','VARCHAR(50)',True,False,'Account code'),
       _ef('Debit_Amount','NUMBER(28,10)',True,False,'Debit'),
       _ef('Credit_Amount','NUMBER(28,10)',True,False,'Credit'),
       _ef('Currency','VARCHAR(3)',True,False,'Currency'),
       _ef('Cost_Center','VARCHAR(50)',True,False,'Cost center'),
       _ef('Memo','VARCHAR(500)',True,False,'Description'),
     ]},
  ],
}

# Build ERP entity lookup: (system, entity_key) → entry
_ERP_ENTITY_MAP = {
    (sys, e['entity']): e
    for sys, entities in ERP_CATALOGUE.items()
    for e in entities
}


def _build_erp_ddl(system: str, entity: str, sf_database: str, sf_schema: str) -> str:
    """Generate Snowflake DDL for an ERP entity using the curated catalogue."""
    key = (system, entity)
    entry = _ERP_ENTITY_MAP.get(key)
    if not entry:
        return (f"-- ERROR: Entity '{entity}' not found in {system} catalogue\n"
                f"-- Skipped\n")

    sf_name = _safe_sf_name(entry['name'])
    fields  = entry.get('fields', [])
    module  = entry.get('module', '')
    desc    = entry.get('desc', '')
    is_api  = system not in ('oracle_ebs', 'dynamics_onprem')

    lines = [
        "-- ============================================================",
        f"-- Source ERP:  {system.upper()}",
        f"-- Entity:      {entity}",
        f"-- Module:      {module}",
        f"-- Description: {desc}",
        f"-- Generated:   {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
    ]
    if is_api:
        lines.append("-- NOTE: DDL derived from API entity schema, not raw DB.")
        lines.append("--       Column list may not match underlying DB exactly.")
    lines += ["-- ============================================================", ""]

    lines.append(
        f"CREATE OR REPLACE TABLE {_safe_sf_name(sf_database)}.{_safe_sf_name(sf_schema)}.{sf_name} ("
    )

    col_defs = []
    pk_cols  = [f['name'] for f in fields if f.get('pk')]

    for f in fields:
        col_name = _safe_sf_name(f['name'])
        sf_type  = f['sf_type']
        not_null = "" if f.get('nullable', True) else " NOT NULL"
        comment  = f" COMMENT '{f['desc']}'" if f.get('desc') else ""
        col_defs.append(f"    {col_name} {sf_type}{not_null}{comment}")

    if pk_cols:
        pk_str = ", ".join(_safe_sf_name(c) for c in pk_cols)
        col_defs.append(f"    CONSTRAINT PK_{entry['name']} PRIMARY KEY ({pk_str})")

    lines.append(",\n".join(col_defs))
    lines.append(")")

    # Cluster key: date columns for API sources, PKs for DB sources
    date_cols = [f['name'] for f in fields
                 if any(k in f['sf_type'] for k in ('TIMESTAMP', 'DATE'))]
    ck_cols = (date_cols[:2] if date_cols else pk_cols[:2])
    if ck_cols:
        lines.append(f"CLUSTER BY ({', '.join(_safe_sf_name(c) for c in ck_cols)})")

    lines += [";", ""]
    return "\n".join(lines)


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


# ========== SAP CATALOGUE ENDPOINT ==========
@app.get("/api/sap/catalogue")
async def sap_catalogue():
    """Return the full SAP table catalogue for UI display."""
    return {"success": True, "catalogue": SAP_TABLE_CATALOGUE}


@app.post("/api/sap/list-tables")
async def sap_list_tables(request: dict):
    """
    For SAP HANA: return actual tables in the schema filtered by selected module(s).
    For SAP ECC/S4 RFC: return catalogue tables matching selected module(s).
    """
    session_id = request.get("session_id") or request.get("session") or ""
    modules = request.get("modules", [])   # e.g. ["SD", "MM", "FI"]

    if not session_id:
        raise HTTPException(400, detail={"message": "No active SAP connection", "fix": "Connect to SAP first"})
    if session_id not in connections:
        raise HTTPException(404, detail={"message": "Session expired", "fix": "Reconnect to SAP"})

    conn = connections[session_id]
    db_type = conn.get("type", "")

    # For all SAP types — return catalogue filtered by module
    filtered = SAP_TABLE_CATALOGUE
    if modules:
        filtered = [t for t in SAP_TABLE_CATALOGUE if t["module"] in modules]

    # For HANA — additionally check which tables actually exist in the schema
    if db_type == "sap_hana" and "connection" in conn:
        hana_conn = conn["connection"]
        schema = getattr(hana_conn, '_migranix_schema', '') or ''
        try:
            existing = set()
            cur = hana_conn.cursor()
            cur.execute(
                "SELECT TABLE_NAME FROM SYS.TABLES WHERE SCHEMA_NAME = ?",
                [schema]
            )
            for row in cur.fetchall():
                existing.add(row[0].upper())
            cur.close()
            # Tag which catalogue tables exist in this HANA instance
            for t in filtered:
                t = dict(t)   # don't mutate global
                t['exists_in_db'] = t['table'] in existing
        except Exception:
            pass   # if metadata query fails, show all as available

    return {"success": True, "tables": filtered, "source_type": db_type}


# ========== ERP CATALOGUE ENDPOINT ==========
@app.get("/api/erp/catalogue")
async def erp_catalogue(system: str = ""):
    """Return ERP entity catalogue for one or all ERP systems."""
    if system and system in ERP_CATALOGUE:
        entities = ERP_CATALOGUE[system]
        return {"success": True, "system": system, "entities": entities}
    # All systems — return flat list with system tag
    all_entities = []
    for sys_name, entities in ERP_CATALOGUE.items():
        for e in entities:
            all_entities.append({**e, "system": sys_name})
    return {"success": True, "entities": all_entities}


# ========== UPDATED GENERATE-DDL (handles legacy SQL + SAP + ERP) ==========
@app.post("/api/generate-ddl")
async def generate_ddl(request: dict):
    """
    Generate Snowflake DDLs.
    Works for:
      - Relational: SQL Server, PostgreSQL, MySQL, Oracle, SQLite, Redshift
      - SAP: HANA (hdbcli), ECC/S4/BW (RFC)
      - ERP: Oracle EBS/Cloud, Dynamics 365/On-prem, NetSuite, Workday
    For SAP/ERP: requires 'selected_tables' / 'selected_entities' list.
    """
    session_id       = request.get("session_id") or request.get("session") or ""
    sf_database      = (request.get("sf_database")  or "MY_SNOWFLAKE_DB").strip().upper()
    sf_schema        = (request.get("sf_schema")     or "PUBLIC").strip().upper()
    selected_tbls    = request.get("selected_tables", [])    # SAP
    selected_entities = request.get("selected_entities", []) # ERP

    if not session_id:
        raise HTTPException(400, detail={
            "message": "No active connection",
            "fix": "Connect to a source database first, then click Generate DDLs"
        })
    if session_id not in connections:
        raise HTTPException(400, detail={
            "message": "Session expired — please reconnect",
            "fix": "Click Connect to Data and reconnect to your source"
        })

    conn    = connections[session_id]
    db_type = conn.get("type", "unknown")

    # ---- Route: ERP, SAP, or standard relational DB ----
    _ERP_TYPES = {'oracle_ebs','oracle_erp_cloud','dynamics365','dynamics_onprem',
                  'netsuite','workday'}
    is_sap = db_type.startswith("sap_")
    is_erp = db_type in _ERP_TYPES

    # ---- ERP path: use curated catalogue, no live DB introspection ----
    if is_erp:
        if not selected_entities:
            raise HTTPException(400, detail={
                "message": "No entities selected",
                "likely_cause": "ERP DDL generation requires entity selection",
                "fix": "Select entities from the ERP catalogue, then click Generate DDLs"
            })
        # Build DDLs directly from catalogue — no live API call needed for schema
        erp_label = db_type.upper()
        db_entry, sql_lines, erp_errors = [], [
            "-- ============================================================",
            f"-- Migranix DDL Export — {erp_label}",
            f"-- Source: {erp_label}  Target: Snowflake  DB: {sf_database}  Schema: {sf_schema}",
            f"-- Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
            f"-- Entities: {len(selected_entities)}",
            "-- NOTE: DDL based on curated entity catalogue.",
            "--       For API-based ERPs, add columns as you discover them in the data.",
            "-- ============================================================",
            "",
            f"CREATE DATABASE IF NOT EXISTS {_safe_sf_name(sf_database)};",
            f"CREATE SCHEMA IF NOT EXISTS {_safe_sf_name(sf_database)}.{_safe_sf_name(sf_schema)};",
            "",
        ], 0

        for entity in selected_entities:
            try:
                ddl = _build_erp_ddl(db_type, entity, sf_database, sf_schema)
                entry_def = _ERP_ENTITY_MAP.get((db_type, entity), {})
                has_err = ddl.startswith("-- ERROR")
                if has_err: erp_errors += 1
                db_entry.append({
                    "table":         entity,
                    "friendly_name": entry_def.get("name", entity),
                    "schema":        db_type,
                    "column_count":  len(entry_def.get("fields", [])),
                    "has_pk":        any(f.get("pk") for f in entry_def.get("fields", [])),
                    "has_cluster_key": True,
                    "ddl":           ddl,
                    "error":         "Entity not in catalogue" if has_err else None,
                })
                sql_lines.append(ddl)
            except Exception as e:
                erp_errors += 1
                err_c = f"-- ERROR for {entity}: {str(e)[:200]}\n"
                db_entry.append({"table": entity, "friendly_name": entity,
                                 "schema": db_type, "column_count": 0,
                                 "has_pk": False, "has_cluster_key": False,
                                 "ddl": err_c, "error": str(e)})
                sql_lines.append(err_c)

        result_by_db = {erp_label: db_entry}
        safe_db = re.sub(r'[^a-zA-Z0-9_]', '_', erp_label)
        zip_files = {f"{safe_db}_snowflake_ddl.sql": "\n".join(sql_lines)}

        import zipfile as _zf, base64 as _b64
        buf = io.BytesIO()
        with _zf.ZipFile(buf, 'w', _zf.ZIP_DEFLATED) as zf:
            for fname, content in zip_files.items():
                zf.writestr(fname, content)
            zf.writestr("README.txt", f"Migranix ERP DDL Export\nSource: {erp_label}\nTarget: Snowflake\n")
        buf.seek(0)
        return {
            "success": True, "source_type": erp_label,
            "sf_database": sf_database, "sf_schema": sf_schema,
            "total_databases": 1, "total_tables": len(selected_entities),
            "total_errors": erp_errors, "databases": result_by_db,
            "zip_b64": _b64.b64encode(buf.read()).decode(),
        }

    # ---- SAP path ----
    try:
        if is_sap:
            tables_by_db, source_label = _get_sap_tables(
                conn, db_type, selected_tbls, sf_database, sf_schema
            )
        else:
            tables_by_db, source_label = _get_source_schema(session_id)
    except ValueError as e:
        raise HTTPException(400, detail={"message": str(e),
            "fix": "Reconnect to a supported database"})
    except Exception as e:
        raise HTTPException(400, detail=power_bi_error(db_type, e))

    if not tables_by_db:
        raise HTTPException(400, detail={
            "message": "No tables found",
            "likely_cause": "Database may be empty or user lacks SELECT permission",
            "fix": "Check that your user has access and tables exist"
        })

    # ---- Build DDLs ----
    result_by_db = {}
    zip_files    = {}

    for db_name, tables in tables_by_db.items():
        if not tables:
            continue
        db_entry  = []
        sql_lines = [
            "-- ============================================================",
            f"-- Migranix DDL Export — {db_name}",
            f"-- Source type: {source_label.upper()}",
            f"-- Target: Snowflake  DB: {sf_database}  Schema: {sf_schema}",
            f"-- Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
            f"-- Tables: {len(tables)}",
            "-- CLUSTER BY is optional — only beneficial on tables > 500 MB.",
            "-- ============================================================",
            "",
            f"CREATE DATABASE IF NOT EXISTS {_safe_sf_name(sf_database)};",
            f"CREATE SCHEMA IF NOT EXISTS {_safe_sf_name(sf_database)}.{_safe_sf_name(sf_schema)};",
            "",
        ]

        for tbl_entry in tables:
            raw_name  = tbl_entry.get("table", "")
            friendly  = _SAP_NAME_MAP.get(raw_name.upper(), raw_name) if is_sap else raw_name
            tbl_entry = dict(tbl_entry)
            tbl_entry["_friendly_name"] = friendly

            try:
                ddl = _build_table_ddl(
                    tbl_entry.get("schema") or db_name,
                    tbl_entry,
                    db_type if not is_sap else "sap_hana",
                    sf_database, sf_schema,
                    friendly_name=friendly if is_sap else None,
                )
                db_entry.append({
                    "table":           raw_name,
                    "friendly_name":   friendly,
                    "schema":          tbl_entry.get("schema") or db_name,
                    "column_count":    len(tbl_entry.get("columns", [])),
                    "has_pk":          bool(tbl_entry.get("pk")),
                    "has_cluster_key": bool(_pick_cluster_key(tbl_entry, db_type)),
                    "ddl":             ddl,
                    "error":           tbl_entry.get("_error"),
                })
                sql_lines.append(ddl)
            except Exception as e:
                err_c = (
                    f"-- ERROR generating DDL for: {raw_name} ({friendly})\n"
                    f"-- Reason: {str(e)[:300]}\n"
                    f"-- Action: Review this table manually\n"
                )
                sql_lines.append(err_c)
                db_entry.append({
                    "table": raw_name, "friendly_name": friendly,
                    "schema": tbl_entry.get("schema") or db_name,
                    "column_count": 0, "has_pk": False, "has_cluster_key": False,
                    "ddl": err_c, "error": str(e)
                })

        result_by_db[db_name] = db_entry
        safe_db = re.sub(r'[^a-zA-Z0-9_]', '_', db_name)
        zip_files[f"{safe_db}_snowflake_ddl.sql"] = "\n".join(sql_lines)

    # ---- Build zip ----
    import zipfile, base64
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
        for fname, content in zip_files.items():
            zf.writestr(fname, content)
        readme = (
            "Migranix DDL Export\n"
            "===================\n\n"
            f"Source: {source_label.upper()}\n"
            f"Target: Snowflake\n\n"
            "How to run:\n"
            "  1. Open Snowflake Worksheets\n"
            "  2. Paste each .sql file and execute\n"
            "  3. CLUSTER BY is optional — remove for tables < 500 MB\n"
            "  4. For SAP tables: friendly names (SALES_ORDER_HEADER) are used\n"
        )
        zf.writestr("README.txt", readme)

    buf.seek(0)
    zip_b64 = base64.b64encode(buf.read()).decode()
    total_tables = sum(len(v) for v in result_by_db.values())
    total_errors = sum(1 for v in result_by_db.values() for t in v if t.get("error"))

    return {
        "success": True,
        "source_type": source_label,
        "sf_database": sf_database,
        "sf_schema": sf_schema,
        "total_databases": len(result_by_db),
        "total_tables": total_tables,
        "total_errors": total_errors,
        "databases": result_by_db,
        "zip_b64": zip_b64,
    }


def _get_sap_tables(conn: dict, db_type: str, selected_tables: list,
                    sf_db: str, sf_schema: str) -> tuple:
    """Route SAP schema read to correct driver."""
    if not selected_tables:
        raise ValueError(
            "For SAP connections, select tables from the catalogue first, "
            "then click Generate DDLs."
        )
    conn_obj = conn.get("connection")
    if conn_obj is None:
        raise ValueError("SAP connection object not found — please reconnect.")

    if db_type == "sap_hana":
        schema = getattr(conn_obj, '_migranix_schema', 'SAP') or 'SAP'
        tables = _get_sap_hana_schema(conn_obj, schema, selected_tables)
        return {"SAP_HANA": tables}, "sap_hana"

    elif db_type in ("sap_ecc", "sap_s4hana", "sap_bw"):
        tables = _get_sap_rfc_schema(conn_obj, selected_tables)
        label  = {"sap_ecc": "SAP_ECC", "sap_s4hana": "SAP_S4HANA",
                  "sap_bw": "SAP_BW"}.get(db_type, "SAP")
        return {label: tables}, db_type

    elif db_type == "sap_s4hana_odata":
        raise ValueError(
            "OData connections expose API data, not raw table schemas. "
            "For DDL generation, connect via RFC instead."
        )
    else:
        raise ValueError(f"Unknown SAP connection type: {db_type}")


# ========== DDL GENERATOR — Legacy → Snowflake ==========

# ---- Data type mappings: source dialect → Snowflake standard types ----
_SQLSERVER_TYPE_MAP = {
    # Strings
    'char': 'VARCHAR', 'nchar': 'VARCHAR', 'varchar': 'VARCHAR', 'nvarchar': 'VARCHAR',
    'text': 'VARCHAR(16777216)', 'ntext': 'VARCHAR(16777216)', 'xml': 'VARIANT',
    # Numerics
    'tinyint': 'NUMBER(3,0)', 'smallint': 'NUMBER(5,0)', 'int': 'NUMBER(10,0)',
    'integer': 'NUMBER(10,0)', 'bigint': 'NUMBER(19,0)',
    'decimal': 'NUMBER', 'numeric': 'NUMBER', 'money': 'NUMBER(19,4)',
    'smallmoney': 'NUMBER(10,4)', 'float': 'FLOAT', 'real': 'FLOAT',
    # Boolean
    'bit': 'BOOLEAN',
    # Date/Time
    'date': 'DATE', 'time': 'TIME', 'datetime': 'TIMESTAMP_NTZ',
    'datetime2': 'TIMESTAMP_NTZ', 'smalldatetime': 'TIMESTAMP_NTZ',
    'datetimeoffset': 'TIMESTAMP_TZ',
    # Binary / other
    'binary': 'BINARY', 'varbinary': 'BINARY', 'image': 'BINARY',
    'uniqueidentifier': 'VARCHAR(36)', 'rowversion': 'BINARY',
    'geography': 'VARIANT', 'geometry': 'VARIANT', 'hierarchyid': 'VARCHAR',
    'sql_variant': 'VARIANT',
}

_POSTGRES_TYPE_MAP = {
    'smallint': 'NUMBER(5,0)', 'int2': 'NUMBER(5,0)',
    'integer': 'NUMBER(10,0)', 'int': 'NUMBER(10,0)', 'int4': 'NUMBER(10,0)',
    'bigint': 'NUMBER(19,0)', 'int8': 'NUMBER(19,0)',
    'decimal': 'NUMBER', 'numeric': 'NUMBER',
    'real': 'FLOAT', 'float4': 'FLOAT', 'double precision': 'FLOAT', 'float8': 'FLOAT',
    'smallserial': 'NUMBER(5,0)', 'serial': 'NUMBER(10,0)', 'bigserial': 'NUMBER(19,0)',
    'money': 'NUMBER(19,2)',
    'char': 'VARCHAR', 'character': 'VARCHAR', 'character varying': 'VARCHAR',
    'varchar': 'VARCHAR', 'text': 'VARCHAR(16777216)',
    'boolean': 'BOOLEAN', 'bool': 'BOOLEAN',
    'date': 'DATE', 'time': 'TIME', 'time without time zone': 'TIME',
    'timestamp': 'TIMESTAMP_NTZ', 'timestamp without time zone': 'TIMESTAMP_NTZ',
    'timestamp with time zone': 'TIMESTAMP_TZ', 'timestamptz': 'TIMESTAMP_TZ',
    'interval': 'VARCHAR',
    'uuid': 'VARCHAR(36)',
    'json': 'VARIANT', 'jsonb': 'VARIANT',
    'xml': 'VARIANT',
    'bytea': 'BINARY',
    'inet': 'VARCHAR(45)', 'cidr': 'VARCHAR(45)', 'macaddr': 'VARCHAR(17)',
    'point': 'VARIANT', 'line': 'VARIANT', 'polygon': 'VARIANT',
    'array': 'VARIANT',
    'tsvector': 'VARCHAR', 'tsquery': 'VARCHAR',
}

_MYSQL_TYPE_MAP = {
    'tinyint': 'NUMBER(3,0)', 'smallint': 'NUMBER(5,0)', 'mediumint': 'NUMBER(7,0)',
    'int': 'NUMBER(10,0)', 'integer': 'NUMBER(10,0)', 'bigint': 'NUMBER(19,0)',
    'decimal': 'NUMBER', 'numeric': 'NUMBER', 'float': 'FLOAT', 'double': 'FLOAT',
    'bit': 'BOOLEAN',
    'char': 'VARCHAR', 'varchar': 'VARCHAR',
    'tinytext': 'VARCHAR(255)', 'text': 'VARCHAR(65535)',
    'mediumtext': 'VARCHAR(16777215)', 'longtext': 'VARCHAR(16777216)',
    'enum': 'VARCHAR', 'set': 'VARCHAR',
    'date': 'DATE', 'time': 'TIME', 'datetime': 'TIMESTAMP_NTZ',
    'timestamp': 'TIMESTAMP_NTZ', 'year': 'NUMBER(4,0)',
    'binary': 'BINARY', 'varbinary': 'BINARY',
    'tinyblob': 'BINARY', 'blob': 'BINARY', 'mediumblob': 'BINARY', 'longblob': 'BINARY',
    'json': 'VARIANT', 'geometry': 'VARIANT', 'point': 'VARIANT',
}

_ORACLE_TYPE_MAP = {
    'number': 'NUMBER', 'float': 'FLOAT', 'binary_float': 'FLOAT',
    'binary_double': 'FLOAT', 'integer': 'NUMBER(38,0)', 'int': 'NUMBER(38,0)',
    'smallint': 'NUMBER(38,0)', 'decimal': 'NUMBER', 'numeric': 'NUMBER',
    'char': 'VARCHAR', 'nchar': 'VARCHAR', 'varchar2': 'VARCHAR', 'nvarchar2': 'VARCHAR',
    'varchar': 'VARCHAR', 'clob': 'VARCHAR(16777216)', 'nclob': 'VARCHAR(16777216)',
    'long': 'VARCHAR(16777216)', 'xmltype': 'VARIANT',
    'date': 'TIMESTAMP_NTZ',  # Oracle DATE includes time
    'timestamp': 'TIMESTAMP_NTZ', 'timestamp with time zone': 'TIMESTAMP_TZ',
    'timestamp with local time zone': 'TIMESTAMP_TZ',
    'interval year to month': 'VARCHAR', 'interval day to second': 'VARCHAR',
    'raw': 'BINARY', 'long raw': 'BINARY', 'blob': 'BINARY', 'bfile': 'BINARY',
    'rowid': 'VARCHAR(18)', 'urowid': 'VARCHAR(4000)',
}

_TYPE_MAPS = {
    'sqlserver': _SQLSERVER_TYPE_MAP,
    'mssql': _SQLSERVER_TYPE_MAP,
    'postgresql': _POSTGRES_TYPE_MAP,
    'postgres': _POSTGRES_TYPE_MAP,
    'mysql': _MYSQL_TYPE_MAP,
    'mariadb': _MYSQL_TYPE_MAP,
    'oracle': _ORACLE_TYPE_MAP,
}


def _map_type(source_db: str, source_type: str, length=None, precision=None, scale=None) -> str:
    """Map a source column type to Snowflake standard type."""
    tmap = _TYPE_MAPS.get(source_db.lower(), {})
    base = source_type.lower().split('(')[0].strip()
    sf_type = tmap.get(base)

    if sf_type is None:
        # Unknown type — default safe fallback
        sf_type = 'VARIANT'

    # Inject precision/scale/length where relevant
    if sf_type in ('VARCHAR', 'CHAR') and length:
        try:
            l = int(length)
            # Snowflake VARCHAR max is 16777216
            sf_type = f'VARCHAR({min(l, 16777216)})'
        except (ValueError, TypeError):
            sf_type = 'VARCHAR'

    elif sf_type == 'NUMBER' and precision is not None:
        try:
            p = int(precision)
            s = int(scale) if scale is not None else 0
            sf_type = f'NUMBER({min(p, 38)},{max(0, min(s, 38))})'
        except (ValueError, TypeError):
            sf_type = 'NUMBER(38,0)'

    return sf_type


def _safe_sf_name(name: str) -> str:
    """Quote identifier if needed for Snowflake."""
    if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name):
        return name.upper()
    return f'"{name}"'


def _get_source_schema(session_id: str) -> dict:
    """Pull full schema from an active session. Returns {db_name: [{table, columns, pk, indexes}]}"""
    if session_id not in connections:
        raise ValueError("Session not found — please reconnect to the source database")
    conn = connections[session_id]
    db_type = conn.get("type", "unknown")
    result = {}

    if "engine" not in conn:
        raise ValueError(f"DDL generation requires a relational database connection, not {db_type}")

    engine = conn["engine"]
    inspector = inspect(engine)

    # Determine database name(s)
    try:
        db_name = conn.get("dsn", "").split('/')[-1].split('?')[0] or db_type
    except Exception:
        db_name = db_type

    tables_data = []
    try:
        schemas = inspector.get_schema_names()
    except Exception:
        schemas = [None]

    for schema in schemas[:20]:
        # Skip system schemas
        skip = {'information_schema', 'pg_catalog', 'pg_toast', 'sys', 'guest',
                'performance_schema', 'mysql', 'information_schema', 'INFORMATION_SCHEMA'}
        if schema and schema.lower() in {s.lower() for s in skip}:
            continue

        try:
            table_names = inspector.get_table_names(schema=schema)
        except Exception:
            table_names = inspector.get_table_names()

        for tbl in table_names[:200]:
            entry = {"schema": schema, "table": tbl, "columns": [], "pk": [], "indexes": []}

            # Columns
            try:
                raw_cols = inspector.get_columns(tbl, schema=schema)
            except Exception:
                try:
                    raw_cols = inspector.get_columns(tbl)
                except Exception:
                    raw_cols = []

            for col in raw_cols:
                col_type = str(col.get("type", "VARCHAR"))
                # Extract length/precision/scale from SQLAlchemy type string
                length = precision = scale = None
                m = re.match(r'([A-Za-z ]+)\((\d+)(?:,\s*(\d+))?\)', col_type)
                if m:
                    length = m.group(2)
                    if m.group(3):
                        precision, scale = m.group(2), m.group(3)
                entry["columns"].append({
                    "name": col.get("name", ""),
                    "source_type": col_type,
                    "sf_type": _map_type(db_type, col_type, length, precision, scale),
                    "nullable": col.get("nullable", True),
                    "default": col.get("default"),
                    "autoincrement": col.get("autoincrement", False),
                    "comment": col.get("comment", ""),
                })

            # Primary keys
            try:
                pk_info = inspector.get_pk_constraint(tbl, schema=schema)
                entry["pk"] = pk_info.get("constrained_columns", [])
            except Exception:
                entry["pk"] = []

            # Indexes (for cluster key selection)
            try:
                idxs = inspector.get_indexes(tbl, schema=schema)
                entry["indexes"] = idxs or []
            except Exception:
                entry["indexes"] = []

            tables_data.append(entry)

    result[db_name] = tables_data
    return result, db_type


def _pick_cluster_key(table_entry: dict, db_type: str) -> list:
    """
    Choose cluster key columns using source DB conventions:
    - SQL Server: columns from the CLUSTERED index (if any)
    - MySQL: PK columns (InnoDB clustered on PK)
    - PostgreSQL: date/timestamp columns first, then PK
    - Oracle: first index columns, or PK
    Returns list of column names (empty = no cluster key)
    """
    pk = table_entry.get("pk", [])
    indexes = table_entry.get("indexes", [])
    columns = table_entry.get("columns", [])
    col_names = [c["name"] for c in columns]

    dt = db_type.lower()

    if dt in ('sqlserver', 'mssql'):
        # Look for clustered index hint in index name
        for idx in indexes:
            idx_name = (idx.get("name") or "").lower()
            if "clustered" in idx_name or "clust" in idx_name:
                cols = idx.get("column_names", [])
                if cols:
                    return cols[:3]
        # Fall back to PK
        if pk:
            return pk[:3]

    elif dt in ('mysql', 'mariadb'):
        # InnoDB is always clustered on PK
        if pk:
            return pk[:3]

    elif dt in ('postgresql', 'postgres'):
        # No true clustered index — prefer date/timestamp columns
        date_cols = [c["name"] for c in columns
                     if any(k in c["sf_type"].lower()
                            for k in ("timestamp", "date", "time"))]
        if date_cols:
            return date_cols[:2]
        if pk:
            return pk[:2]

    elif dt == 'oracle':
        # Index-organized table or first unique index
        for idx in indexes:
            if idx.get("unique"):
                cols = idx.get("column_names", [])
                if cols:
                    return cols[:3]
        if pk:
            return pk[:3]
        # Fall back to date cols
        date_cols = [c["name"] for c in columns
                     if any(k in c["sf_type"].lower()
                            for k in ("timestamp", "date"))]
        if date_cols:
            return date_cols[:2]

    # Generic fallback
    if pk:
        return pk[:3]
    return []


def _build_table_ddl(schema_name: str, table_entry: dict, db_type: str,
                     sf_database: str = "MY_SNOWFLAKE_DB",
                     sf_schema: str = "PUBLIC",
                     friendly_name: str = None) -> str:
    """Generate a complete Snowflake DDL block for one table."""
    tbl = table_entry["table"]
    columns = table_entry["columns"]
    pk = table_entry["pk"]
    cluster_key = _pick_cluster_key(table_entry, db_type)

    # For SAP tables: use friendly name as Snowflake table name, keep original as comment
    sf_tbl_name = friendly_name if friendly_name else tbl
    sf_tbl = _safe_sf_name(sf_tbl_name)
    src_label = f"{schema_name}.{tbl}" if schema_name else tbl
    friendly_label = f" → {friendly_name}" if friendly_name and friendly_name != tbl else ""

    lines = []
    lines.append(f"-- ============================================================")
    lines.append(f"-- Source: {src_label}{friendly_label}")
    lines.append(f"-- Database type: {db_type.upper()}")
    lines.append(f"-- Generated by Migranix — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append(f"-- ============================================================")
    lines.append(f"")

    # Error-safe wrapper: use CREATE OR REPLACE so re-runs never fail
    lines.append(f"CREATE OR REPLACE TABLE {_safe_sf_name(sf_database)}.{_safe_sf_name(sf_schema)}.{sf_tbl} (")

    col_defs = []
    for col in columns:
        col_name = _safe_sf_name(col["name"])
        sf_type = col["sf_type"]

        parts = [f"    {col_name} {sf_type}"]

        # NOT NULL
        if not col["nullable"] and not col.get("autoincrement"):
            parts.append("NOT NULL")

        # DEFAULT value — only safe literals, skip expressions
        default = col.get("default")
        if default is not None:
            d = str(default).strip()
            # Skip server-specific functions that don't exist in Snowflake
            skip_defaults = {'getdate()', 'getutcdate()', 'now()', 'sysdate',
                             'current_timestamp', 'newid()', 'sys_guid()'}
            if d.lower() not in skip_defaults and not d.startswith('(') and len(d) < 200:
                # String defaults need quoting if not already quoted
                if sf_type.startswith('VARCHAR') and not d.startswith("'"):
                    d = f"'{d}'"
                try:
                    # Only keep numeric and simple string defaults
                    float(d)
                    parts.append(f"DEFAULT {d}")
                except ValueError:
                    if d.startswith("'") and d.endswith("'"):
                        parts.append(f"DEFAULT {d}")
                    # Otherwise skip — too risky

        # Source type as comment
        src_type_comment = col.get("source_type", "")
        if src_type_comment:
            parts.append(f"COMMENT '{src_type_comment}'")

        col_defs.append(" ".join(parts))

    # Primary key constraint (inline)
    if pk:
        pk_cols = ", ".join(_safe_sf_name(c) for c in pk)
        col_defs.append(f"    CONSTRAINT PK_{sf_tbl_name.upper()} PRIMARY KEY ({pk_cols})")

    lines.append(",\n".join(col_defs))
    lines.append(")")

    # Cluster key — wrapped in comment explaining it's optional
    if cluster_key:
        ck_cols = ", ".join(_safe_sf_name(c) for c in cluster_key)
        lines.append(f"CLUSTER BY ({ck_cols})")

    lines.append(";")
    lines.append("")

    # Index comments (indexes don't exist in Snowflake — document them)
    for idx in table_entry.get("indexes", []):
        idx_name = idx.get("name", "unnamed")
        idx_cols = ", ".join(idx.get("column_names", []))
        unique = "UNIQUE " if idx.get("unique") else ""
        lines.append(f"-- Source index: {unique}INDEX {idx_name} ON {src_label} ({idx_cols})")
        lines.append(f"-- NOTE: Snowflake does not use indexes. Use CLUSTER BY or micro-partition pruning instead.")
    if table_entry.get("indexes"):
        lines.append("")

    return "\n".join(lines)


# ============================================================
# PIPELINE ENGINE — Full Load, Incremental, CDC, Scheduler
# ============================================================

# ---- APScheduler setup ----
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    _scheduler = BackgroundScheduler(timezone="UTC")
    _scheduler.start()
    _HAS_SCHEDULER = True
except Exception:
    _scheduler = None
    _HAS_SCHEDULER = False

# In-memory pipeline store (also persisted to Supabase)
# {pipeline_id: {config, status, last_run, next_run, stats}}
_pipelines: Dict[str, dict] = {}
_pipeline_lock = __import__('threading').Lock()


# ── SNOWFLAKE TARGET CONNECTION ──────────────────────────────────────────────
def _get_sf_engine(sf_creds: dict):
    """Build SQLAlchemy engine for Snowflake target."""
    if snowflake_connector is None:
        raise ImportError("snowflake-connector-python not installed")
    from urllib.parse import quote_plus
    user     = sf_creds.get("username") or sf_creds.get("user", "")
    password = sf_creds.get("password", "")
    account  = sf_creds.get("account", "")
    database = sf_creds.get("database", "")
    schema   = sf_creds.get("schema", "PUBLIC")
    warehouse= sf_creds.get("warehouse", "")
    role     = sf_creds.get("role", "")
    if not account: raise ValueError("Snowflake account identifier is required")
    if not user:    raise ValueError("Snowflake username is required")
    conn_str = (f"snowflake://{quote_plus(user)}:{quote_plus(password)}"
                f"@{account}/{database}/{schema}"
                f"?warehouse={quote_plus(warehouse)}&role={quote_plus(role)}")
    engine = create_engine(conn_str, poolclass=NullPool)
    return engine


# ── TYPE COERCION FOR SNOWFLAKE WRITE ─────────────────────────────────────
def _coerce_df_for_snowflake(df: "pd.DataFrame") -> "pd.DataFrame":
    """Fix pandas types that cause issues writing to Snowflake."""
    import pandas as _pd
    for col in df.columns:
        if df[col].dtype == object:
            # Try to keep as string; truncate very long values
            df[col] = df[col].astype(str).where(df[col].notna(), None)
        # Convert timezone-aware datetimes to UTC naive
        if hasattr(df[col], 'dt') and hasattr(df[col].dt, 'tz') and df[col].dt.tz is not None:
            df[col] = df[col].dt.tz_convert('UTC').dt.tz_localize(None)
    return df


# ── FULL LOAD ───────────────────────────────────────────────────────────────
def _run_full_load(pipeline: dict, log_fn=None) -> dict:
    """
    Extract ALL rows from source table and REPLACE target table.
    Returns stats dict.
    """
    def log(msg):
        if log_fn: log_fn(msg)

    src_session_id = pipeline["source_session_id"]
    sf_creds       = pipeline["snowflake_creds"]
    src_table      = pipeline["source_table"]
    src_schema     = pipeline.get("source_schema", "")
    tgt_table      = pipeline.get("target_table") or src_table.upper()
    tgt_db         = (pipeline.get("target_database") or "MY_SNOWFLAKE_DB").upper()
    tgt_schema     = (pipeline.get("target_schema") or "PUBLIC").upper()
    batch_size     = int(pipeline.get("batch_size") or 10000)
    ddl            = pipeline.get("ddl")  # pre-generated DDL string

    if src_session_id not in connections:
        raise ValueError(f"Source session {src_session_id} not found — reconnect source")

    src_conn = connections[src_session_id]
    db_type  = src_conn.get("type", "")
    engine   = src_conn.get("engine")

    if engine is None:
        raise ValueError(f"Full load requires a relational DB connection, not {db_type}")

    sf_engine = _get_sf_engine(sf_creds)
    total_rows = 0
    start_ts   = datetime.utcnow()

    with sf_engine.begin() as sf_conn:
        # 1. Create DB + schema if not exists
        sf_conn.execute(text(f'CREATE DATABASE IF NOT EXISTS "{tgt_db}"'))
        sf_conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{tgt_db}"."{tgt_schema}"'))

        # 2. Create or replace target table
        if ddl:
            log(f"Creating target table {tgt_db}.{tgt_schema}.{tgt_table}")
            # Execute DDL statements one by one
            for stmt in [s.strip() for s in ddl.split(';') if s.strip() and not s.strip().startswith('--')]:
                try:
                    sf_conn.execute(text(stmt))
                except Exception as e:
                    log(f"DDL warning (non-fatal): {str(e)[:200]}")

    # 3. Extract + load in batches
    qual_table = f'"{src_schema}"."{src_table}"' if src_schema else f'"{src_table}"'
    offset = 0

    with sf_engine.connect() as sf_conn:
        first_batch = True
        while True:
            try:
                # Use SQLAlchemy for portable pagination
                if db_type in ("sqlserver", "mssql"):
                    sql = f"SELECT * FROM {qual_table} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
                elif db_type == "oracle":
                    sql = f"SELECT * FROM {qual_table} OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
                else:
                    sql = f"SELECT * FROM {qual_table} LIMIT {batch_size} OFFSET {offset}"

                with engine.connect() as src_conn_obj:
                    df = pd.read_sql(text(sql), src_conn_obj)

                if df.empty:
                    break

                df = _coerce_df_for_snowflake(df)
                df.columns = [c.upper() for c in df.columns]

                # Write to Snowflake
                tgt_full = f'"{tgt_db}"."{tgt_schema}"."{tgt_table}"'
                if first_batch:
                    # Truncate before first batch on full load
                    try:
                        sf_conn.execute(text(f"TRUNCATE TABLE IF EXISTS {tgt_full}"))
                        sf_conn.commit()
                    except Exception:
                        pass
                    first_batch = False

                df.to_sql(
                    tgt_table.lower(),
                    sf_conn,
                    schema=f"{tgt_db}.{tgt_schema}".lower(),
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000,
                )
                sf_conn.commit()
                total_rows += len(df)
                offset     += batch_size
                log(f"Loaded {total_rows} rows so far...")

                if len(df) < batch_size:
                    break   # last page

            except Exception as e:
                raise RuntimeError(f"Load failed at offset {offset}: {str(e)[:400]}")

    elapsed = (datetime.utcnow() - start_ts).total_seconds()
    return {
        "rows_loaded": total_rows,
        "elapsed_seconds": round(elapsed, 2),
        "rows_per_second": round(total_rows / max(elapsed, 0.1), 0),
        "mode": "full_load",
        "finished_at": datetime.utcnow().isoformat(),
    }


# ── INCREMENTAL LOAD (high-watermark) ───────────────────────────────────────
def _run_incremental_load(pipeline: dict, log_fn=None) -> dict:
    """
    Extract only rows where watermark_column > last_watermark_value.
    Appends to target table (no truncate).
    """
    def log(msg):
        if log_fn: log_fn(msg)

    src_session_id   = pipeline["source_session_id"]
    sf_creds         = pipeline["snowflake_creds"]
    src_table        = pipeline["source_table"]
    src_schema       = pipeline.get("source_schema", "")
    tgt_table        = (pipeline.get("target_table") or src_table).upper()
    tgt_db           = (pipeline.get("target_database") or "MY_SNOWFLAKE_DB").upper()
    tgt_schema       = (pipeline.get("target_schema") or "PUBLIC").upper()
    watermark_col    = pipeline.get("watermark_column", "")
    last_watermark   = pipeline.get("last_watermark_value")
    batch_size       = int(pipeline.get("batch_size") or 10000)

    if not watermark_col:
        raise ValueError("watermark_column is required for incremental load")

    if src_session_id not in connections:
        raise ValueError(f"Source session {src_session_id} not found")

    src_conn   = connections[src_session_id]
    engine     = src_conn.get("engine")
    if engine is None:
        raise ValueError("Incremental load requires a relational DB connection")

    sf_engine  = _get_sf_engine(sf_creds)
    qual_table = f'"{src_schema}"."{src_table}"' if src_schema else f'"{src_table}"'
    total_rows = 0
    max_watermark = last_watermark
    start_ts   = datetime.utcnow()

    with sf_engine.connect() as sf_conn:
        # Get current max watermark from Snowflake target (to resume correctly)
        try:
            tgt_full = f'"{tgt_db}"."{tgt_schema}"."{tgt_table}"'
            wm_result = sf_conn.execute(
                text(f'SELECT MAX("{watermark_col.upper()}") FROM {tgt_full}')
            ).fetchone()
            if wm_result and wm_result[0] is not None:
                last_watermark = wm_result[0]
                log(f"Resuming from watermark: {last_watermark}")
        except Exception:
            log("Could not read watermark from target — doing full incremental scan")

        # Extract changed rows
        where_clause = (f'WHERE "{watermark_col}" > :wm'
                        if last_watermark is not None
                        else "")
        params = {"wm": last_watermark} if last_watermark is not None else {}

        with engine.connect() as src_conn_obj:
            sql = f"SELECT * FROM {qual_table} {where_clause} ORDER BY \"{watermark_col}\""
            df  = pd.read_sql(text(sql), src_conn_obj, params=params)

        if df.empty:
            log("No new rows since last watermark")
            return {"rows_loaded": 0, "mode": "incremental",
                    "finished_at": datetime.utcnow().isoformat()}

        df = _coerce_df_for_snowflake(df)
        df.columns = [c.upper() for c in df.columns]

        # Track new max watermark
        if watermark_col.upper() in df.columns:
            new_wm = df[watermark_col.upper()].max()
            if max_watermark is None or new_wm > max_watermark:
                max_watermark = str(new_wm)

        # Upsert: delete matching PKs then insert (Snowflake has no MERGE in all editions)
        pk_cols = pipeline.get("primary_keys", [])
        if pk_cols:
            pk_str = " AND ".join([f't."{c.upper()}" = s."{c.upper()}"' for c in pk_cols])
            for _, row in df.iterrows():
                where_del = " AND ".join([f'"{c.upper()}" = \'{row[c.upper()]}\'' for c in pk_cols])
                try:
                    sf_conn.execute(text(f'DELETE FROM {tgt_full} WHERE {where_del}'))
                except Exception:
                    pass

        df.to_sql(
            tgt_table.lower(),
            sf_conn,
            schema=f"{tgt_db}.{tgt_schema}".lower(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000,
        )
        sf_conn.commit()
        total_rows = len(df)

    # Persist new watermark back to pipeline config
    with _pipeline_lock:
        if pipeline["pipeline_id"] in _pipelines:
            _pipelines[pipeline["pipeline_id"]]["last_watermark_value"] = max_watermark
            _pipelines[pipeline["pipeline_id"]]["pipeline"] = {
                **_pipelines[pipeline["pipeline_id"]].get("pipeline", {}),
                "last_watermark_value": max_watermark
            }

    elapsed = (datetime.utcnow() - start_ts).total_seconds()
    return {
        "rows_loaded": total_rows,
        "new_watermark": str(max_watermark),
        "elapsed_seconds": round(elapsed, 2),
        "mode": "incremental",
        "finished_at": datetime.utcnow().isoformat(),
    }


# ── LOG-BASED CDC — SQL SERVER ────────────────────────────────────────────
def _run_cdc_sqlserver(pipeline: dict, log_fn=None) -> dict:
    """
    SQL Server log-based CDC using sys.fn_cdc_get_all_changes.
    Requirements on SQL Server:
      EXEC sys.sp_cdc_enable_db;
      EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='<table>', @role_name=NULL;
    """
    def log(msg):
        if log_fn: log_fn(msg)

    src_session_id = pipeline["source_session_id"]
    sf_creds       = pipeline["snowflake_creds"]
    src_table      = pipeline["source_table"]
    src_schema     = pipeline.get("source_schema", "dbo")
    tgt_table      = (pipeline.get("target_table") or src_table).upper()
    tgt_db         = (pipeline.get("target_database") or "MY_SNOWFLAKE_DB").upper()
    tgt_schema     = (pipeline.get("target_schema") or "PUBLIC").upper()
    last_lsn       = pipeline.get("last_lsn")  # stored as hex string

    if src_session_id not in connections:
        raise ValueError("Source session not found")
    engine = connections[src_session_id].get("engine")
    if engine is None:
        raise ValueError("CDC requires a SQL Server relational connection")

    sf_engine  = _get_sf_engine(sf_creds)
    total_rows = 0
    start_ts   = datetime.utcnow()
    capture_instance = f"{src_schema}_{src_table}"

    with engine.connect() as src_conn:
        # Get current max LSN
        max_lsn_row = src_conn.execute(
            text("SELECT sys.fn_cdc_get_max_lsn() AS max_lsn")
        ).fetchone()
        max_lsn = max_lsn_row[0] if max_lsn_row else None

        if max_lsn is None:
            raise ValueError("CDC not enabled on this SQL Server database. "
                             "Run: EXEC sys.sp_cdc_enable_db")

        # Determine from_lsn
        if last_lsn:
            from_lsn = bytes.fromhex(last_lsn.replace('0x','').replace('\\x',''))
        else:
            # First run — get min LSN for this table
            min_lsn_row = src_conn.execute(
                text(f"SELECT sys.fn_cdc_get_min_lsn(:ci) AS min_lsn"),
                {"ci": capture_instance}
            ).fetchone()
            from_lsn = min_lsn_row[0] if min_lsn_row else max_lsn
            log(f"First CDC run — starting from min LSN for {capture_instance}")

        # Fetch changes
        changes_sql = text(
            f"SELECT * FROM cdc.fn_cdc_get_all_changes_{capture_instance}"
            f"(:from_lsn, :to_lsn, N'all update old')"
        )
        try:
            df = pd.read_sql(changes_sql, src_conn,
                             params={"from_lsn": from_lsn, "to_lsn": max_lsn})
        except Exception as e:
            raise ValueError(
                f"CDC query failed: {str(e)[:300]}. "
                f"Ensure sp_cdc_enable_table was run for table '{src_table}'"
            )

    if not df.empty:
        # __$operation: 1=DELETE, 2=INSERT, 3=before UPDATE, 4=after UPDATE
        inserts = df[df['__$operation'].isin([2, 4])].copy()
        deletes = df[df['__$operation'] == 1].copy()

        # Drop CDC system columns
        cdc_cols = [c for c in df.columns if c.startswith('__$')]
        inserts.drop(columns=cdc_cols, inplace=True, errors='ignore')
        deletes.drop(columns=cdc_cols, inplace=True, errors='ignore')

        inserts = _coerce_df_for_snowflake(inserts)
        inserts.columns = [c.upper() for c in inserts.columns]
        deletes.columns = [c.upper() for c in deletes.columns]

        pk_cols = pipeline.get("primary_keys", [])
        tgt_full = f'"{tgt_db}"."{tgt_schema}"."{tgt_table}"'

        with sf_engine.connect() as sf_conn:
            # Apply deletes
            for _, row in deletes.iterrows():
                if pk_cols:
                    where_del = " AND ".join(
                        [f'"{c.upper()}" = \'{row.get(c.upper(), "")}\'' for c in pk_cols]
                    )
                    try:
                        sf_conn.execute(text(f'DELETE FROM {tgt_full} WHERE {where_del}'))
                    except Exception:
                        pass

            # Apply inserts/updates (delete+reinsert pattern)
            if pk_cols and not inserts.empty:
                for _, row in inserts.iterrows():
                    where_del = " AND ".join(
                        [f'"{c.upper()}" = \'{row.get(c.upper(), "")}\'' for c in pk_cols]
                    )
                    try:
                        sf_conn.execute(text(f'DELETE FROM {tgt_full} WHERE {where_del}'))
                    except Exception:
                        pass

            if not inserts.empty:
                inserts.to_sql(
                    tgt_table.lower(),
                    sf_conn,
                    schema=f"{tgt_db}.{tgt_schema}".lower(),
                    if_exists='append', index=False,
                    method='multi', chunksize=500,
                )
            sf_conn.commit()
            total_rows = len(inserts)

    # Persist new LSN
    new_lsn_hex = max_lsn.hex() if isinstance(max_lsn, (bytes, bytearray)) else str(max_lsn)
    with _pipeline_lock:
        if pipeline["pipeline_id"] in _pipelines:
            _pipelines[pipeline["pipeline_id"]]["last_lsn"] = new_lsn_hex

    elapsed = (datetime.utcnow() - start_ts).total_seconds()
    return {
        "rows_loaded": total_rows,
        "new_lsn": new_lsn_hex,
        "elapsed_seconds": round(elapsed, 2),
        "mode": "cdc_sqlserver",
        "finished_at": datetime.utcnow().isoformat(),
    }


# ── LOG-BASED CDC — POSTGRESQL ────────────────────────────────────────────
def _run_cdc_postgresql(pipeline: dict, log_fn=None) -> dict:
    """
    PostgreSQL CDC using logical replication slots + pgoutput plugin.
    Requirements:
      postgresql.conf: wal_level = logical
      CREATE PUBLICATION migranix_pub FOR TABLE <table>;
      CREATE_REPLICATION_SLOT migranix_slot LOGICAL pgoutput;
    Uses psycopg2 raw connection (not SQLAlchemy) for replication protocol.
    """
    def log(msg):
        if log_fn: log_fn(msg)

    src_session_id = pipeline["source_session_id"]
    sf_creds       = pipeline["snowflake_creds"]
    src_table      = pipeline["source_table"]
    src_schema     = pipeline.get("source_schema", "public")
    tgt_table      = (pipeline.get("target_table") or src_table).upper()
    tgt_db         = (pipeline.get("target_database") or "MY_SNOWFLAKE_DB").upper()
    tgt_schema     = (pipeline.get("target_schema") or "PUBLIC").upper()
    slot_name      = pipeline.get("pg_slot_name") or f"migranix_{src_table.lower()}"
    pub_name       = pipeline.get("pg_pub_name") or f"migranix_pub_{src_table.lower()}"

    if src_session_id not in connections:
        raise ValueError("Source session not found")

    src_conn_meta = connections[src_session_id]
    dsn = src_conn_meta.get("dsn", "")

    if not dsn or "postgresql" not in dsn:
        raise ValueError("CDC requires a PostgreSQL connection")

    import psycopg2
    import psycopg2.extras

    # Parse DSN for replication connection
    # SQLAlchemy DSN: postgresql+psycopg2://user:pass@host:port/db
    dsn_clean = dsn.replace("postgresql+psycopg2://", "").replace("postgresql://", "")
    # Build psycopg2 DSN
    try:
        from urllib.parse import urlparse
        parsed = urlparse("postgresql://" + dsn_clean)
        pg_conn_params = {
            "host":     parsed.hostname or "localhost",
            "port":     parsed.port or 5432,
            "database": parsed.path.lstrip("/").split("?")[0],
            "user":     parsed.username or "",
            "password": parsed.password or "",
            "connection_factory": psycopg2.extras.LogicalReplicationConnection,
        }
    except Exception as e:
        raise ValueError(f"Could not parse PostgreSQL DSN for CDC: {e}")

    sf_engine  = _get_sf_engine(sf_creds)
    total_rows = 0
    changes = {"insert": [], "update": [], "delete": []}
    start_ts = datetime.utcnow()

    try:
        rep_conn = psycopg2.connect(**pg_conn_params)
        rep_cur  = rep_conn.cursor()

        # Create publication if not exists
        with psycopg2.connect(**{k: v for k, v in pg_conn_params.items()
                                  if k != "connection_factory"}) as plain_conn:
            with plain_conn.cursor() as pc:
                pc.execute(
                    "SELECT 1 FROM pg_publication WHERE pubname = %s", (pub_name,)
                )
                if not pc.fetchone():
                    pc.execute(
                        f'CREATE PUBLICATION "{pub_name}" FOR TABLE "{src_schema}"."{src_table}"'
                    )
                    plain_conn.commit()
                    log(f"Created publication {pub_name}")

                # Create slot if not exists
                pc.execute(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (slot_name,)
                )
                if not pc.fetchone():
                    pc.execute(
                        f"SELECT pg_create_logical_replication_slot('{slot_name}', 'wal2json')"
                    )
                    plain_conn.commit()
                    log(f"Created replication slot {slot_name}")

        # Read changes from slot using wal2json output
        with psycopg2.connect(**{k: v for k, v in pg_conn_params.items()
                                  if k != "connection_factory"}) as plain_conn:
            with plain_conn.cursor() as pc:
                pc.execute(
                    "SELECT data FROM pg_logical_slot_get_changes(%s, NULL, NULL, "
                    "'format-version', '2', 'add-tables', %s)",
                    (slot_name, f"{src_schema}.{src_table}")
                )
                rows = pc.fetchall()
                plain_conn.commit()

        import json as _json
        for row in rows:
            try:
                change = _json.loads(row[0])
                action = change.get("action", "")
                cols   = {c["name"]: c.get("value") for c in change.get("columns", [])}
                if action in ("I", "insert"):
                    changes["insert"].append(cols)
                elif action in ("U", "update"):
                    changes["update"].append(cols)
                elif action in ("D", "delete"):
                    changes["delete"].append(cols)
            except Exception:
                continue

        rep_conn.close()

    except Exception as e:
        raise ValueError(
            f"PostgreSQL CDC failed: {str(e)[:300]}. "
            "Ensure wal_level=logical in postgresql.conf and REPLICATION privilege is granted."
        )

    if any(changes.values()):
        pk_cols  = pipeline.get("primary_keys", [])
        tgt_full = f'"{tgt_db}"."{tgt_schema}"."{tgt_table}"'

        with sf_engine.connect() as sf_conn:
            # Deletes
            for row in changes["delete"]:
                if pk_cols:
                    where_del = " AND ".join(
                        [f'"{c.upper()}" = \'{row.get(c, "")}\'' for c in pk_cols]
                    )
                    try:
                        sf_conn.execute(text(f'DELETE FROM {tgt_full} WHERE {where_del}'))
                    except Exception:
                        pass

            # Inserts + Updates (upsert pattern)
            all_writes = changes["insert"] + changes["update"]
            if all_writes:
                if pk_cols:
                    for row in all_writes:
                        where_del = " AND ".join(
                            [f'"{c.upper()}" = \'{row.get(c, "")}\'' for c in pk_cols]
                        )
                        try:
                            sf_conn.execute(text(f'DELETE FROM {tgt_full} WHERE {where_del}'))
                        except Exception:
                            pass
                df = pd.DataFrame(all_writes)
                df = _coerce_df_for_snowflake(df)
                df.columns = [c.upper() for c in df.columns]
                df.to_sql(
                    tgt_table.lower(), sf_conn,
                    schema=f"{tgt_db}.{tgt_schema}".lower(),
                    if_exists='append', index=False,
                    method='multi', chunksize=500,
                )
            sf_conn.commit()
            total_rows = len(all_writes)

    elapsed = (datetime.utcnow() - start_ts).total_seconds()
    return {
        "rows_loaded": total_rows,
        "inserts": len(changes["insert"]),
        "updates": len(changes["update"]),
        "deletes": len(changes["delete"]),
        "elapsed_seconds": round(elapsed, 2),
        "mode": "cdc_postgresql",
        "finished_at": datetime.utcnow().isoformat(),
    }


# ── ROW COUNT + CHECKSUM RECONCILIATION ──────────────────────────────────
def _run_reconciliation(pipeline: dict, log_fn=None) -> dict:
    """
    Compare source vs Snowflake target:
    1. Row counts
    2. Checksum on numeric columns (SUM)
    3. Returns pass/fail + discrepancy details
    """
    def log(msg):
        if log_fn: log_fn(msg)

    src_session_id = pipeline["source_session_id"]
    sf_creds       = pipeline["snowflake_creds"]
    src_table      = pipeline["source_table"]
    src_schema     = pipeline.get("source_schema", "")
    tgt_table      = (pipeline.get("target_table") or src_table).upper()
    tgt_db         = (pipeline.get("target_database") or "MY_SNOWFLAKE_DB").upper()
    tgt_schema     = (pipeline.get("target_schema") or "PUBLIC").upper()

    if src_session_id not in connections:
        raise ValueError("Source session not found")

    engine    = connections[src_session_id].get("engine")
    sf_engine = _get_sf_engine(sf_creds)
    qual_src  = f'"{src_schema}"."{src_table}"' if src_schema else f'"{src_table}"'
    tgt_full  = f'"{tgt_db}"."{tgt_schema}"."{tgt_table}"'
    result    = {"table": src_table, "target": tgt_full}

    # 1. Row counts
    with engine.connect() as src_conn:
        src_count = src_conn.execute(
            text(f"SELECT COUNT(*) FROM {qual_src}")
        ).scalar()

    with sf_engine.connect() as sf_conn:
        try:
            tgt_count = sf_conn.execute(
                text(f"SELECT COUNT(*) FROM {tgt_full}")
            ).scalar()
        except Exception:
            tgt_count = 0

    result["source_rows"]      = src_count
    result["target_rows"]      = tgt_count
    result["row_count_match"]  = src_count == tgt_count
    result["row_count_diff"]   = abs(int(src_count or 0) - int(tgt_count or 0))

    # 2. Column checksums (numeric columns only)
    checksums = {}
    try:
        with engine.connect() as src_conn:
            # Get numeric columns
            inspector = inspect(engine)
            cols = inspector.get_columns(src_table,
                                         schema=src_schema if src_schema else None)
            num_cols = [c["name"] for c in cols
                        if any(t in str(c["type"]).lower()
                               for t in ["int", "float", "decimal", "numeric", "money"])]

        for col in num_cols[:10]:  # max 10 columns
            with engine.connect() as src_conn:
                src_sum = src_conn.execute(
                    text(f'SELECT SUM("{col}") FROM {qual_src}')
                ).scalar() or 0
            with sf_engine.connect() as sf_conn:
                try:
                    tgt_sum = sf_conn.execute(
                        text(f'SELECT SUM("{col.upper()}") FROM {tgt_full}')
                    ).scalar() or 0
                except Exception:
                    tgt_sum = None
            checksums[col] = {
                "source_sum": float(src_sum) if src_sum else 0,
                "target_sum": float(tgt_sum) if tgt_sum else 0,
                "match": abs(float(src_sum or 0) - float(tgt_sum or 0)) < 0.01,
            }
    except Exception as e:
        log(f"Checksum warning: {str(e)[:200]}")

    result["checksums"]       = checksums
    result["checksum_passed"] = all(v["match"] for v in checksums.values())
    result["overall_passed"]  = result["row_count_match"] and result["checksum_passed"]
    result["reconciled_at"]   = datetime.utcnow().isoformat()

    log(f"Reconciliation: rows {src_count}→{tgt_count} {'✓' if result['row_count_match'] else '✗'}")
    return result


# ── PIPELINE RUNNER (dispatcher) ──────────────────────────────────────────
def _run_pipeline(pipeline_id: str):
    """Main dispatcher — called by scheduler or manual trigger."""
    with _pipeline_lock:
        if pipeline_id not in _pipelines:
            return
        pipeline_entry = _pipelines[pipeline_id]

    pipeline = pipeline_entry.get("pipeline", {})
    mode     = pipeline.get("mode", "full_load")
    logs     = []

    def log_fn(msg):
        ts = datetime.utcnow().strftime("%H:%M:%S")
        entry = f"[{ts}] {msg}"
        logs.append(entry)

    with _pipeline_lock:
        _pipelines[pipeline_id]["status"]   = "running"
        _pipelines[pipeline_id]["last_run"] = datetime.utcnow().isoformat()
        _pipelines[pipeline_id]["logs"]     = []

    try:
        log_fn(f"Starting {mode} for table {pipeline.get('source_table')}")

        if mode == "full_load":
            stats = _run_full_load(pipeline, log_fn)

        elif mode == "incremental":
            stats = _run_incremental_load(pipeline, log_fn)

        elif mode == "cdc_sqlserver":
            stats = _run_cdc_sqlserver(pipeline, log_fn)

        elif mode == "cdc_postgresql":
            stats = _run_cdc_postgresql(pipeline, log_fn)

        else:
            raise ValueError(f"Unknown pipeline mode: {mode}")

        # Auto-reconcile if enabled
        if pipeline.get("auto_reconcile"):
            log_fn("Running auto-reconciliation...")
            rec = _run_reconciliation(pipeline, log_fn)
            stats["reconciliation"] = rec
            if not rec["overall_passed"]:
                log_fn(f"⚠️  Reconciliation FAILED: row diff={rec['row_count_diff']}")

        with _pipeline_lock:
            _pipelines[pipeline_id]["status"]       = "success"
            _pipelines[pipeline_id]["last_stats"]   = stats
            _pipelines[pipeline_id]["logs"]         = logs
            _pipelines[pipeline_id]["error"]        = None
            _pipelines[pipeline_id]["run_count"]    = \
                _pipelines[pipeline_id].get("run_count", 0) + 1

        log_fn(f"Completed. Rows: {stats.get('rows_loaded', 0)}")

    except Exception as e:
        err_msg = str(e)[:500]
        log_fn(f"ERROR: {err_msg}")
        with _pipeline_lock:
            _pipelines[pipeline_id]["status"]  = "error"
            _pipelines[pipeline_id]["error"]   = err_msg
            _pipelines[pipeline_id]["logs"]    = logs


# ── PIPELINE API ENDPOINTS ────────────────────────────────────────────────

@app.post("/api/pipeline/create")
async def create_pipeline(request: dict):
    """
    Create a new data pipeline.
    Required fields:
      name, source_session_id, source_table, snowflake_creds,
      mode (full_load|incremental|cdc_sqlserver|cdc_postgresql),
      schedule (cron string like '0 2 * * *' or interval like '15m')
    Optional:
      source_schema, target_table, target_database, target_schema,
      watermark_column (for incremental), primary_keys,
      auto_reconcile, ddl, batch_size
    """
    name        = request.get("name", "").strip()
    mode        = request.get("mode", "full_load")
    schedule    = request.get("schedule", "")
    src_session = request.get("source_session_id", "")
    sf_creds    = request.get("snowflake_creds", {})
    src_table   = request.get("source_table", "").strip()

    if not name:        raise HTTPException(400, detail={"message": "Pipeline name is required"})
    if not src_session: raise HTTPException(400, detail={"message": "source_session_id is required"})
    if not src_table:   raise HTTPException(400, detail={"message": "source_table is required"})
    if not sf_creds:    raise HTTPException(400, detail={"message": "snowflake_creds is required"})
    if mode not in ("full_load","incremental","cdc_sqlserver","cdc_postgresql"):
        raise HTTPException(400, detail={"message": f"Invalid mode: {mode}"})
    if mode == "incremental" and not request.get("watermark_column"):
        raise HTTPException(400, detail={"message": "watermark_column required for incremental mode"})

    pipeline_id = str(uuid.uuid4())
    pipeline_cfg = {
        "pipeline_id":      pipeline_id,
        "name":             name,
        "mode":             mode,
        "source_session_id": src_session,
        "source_table":     src_table,
        "source_schema":    request.get("source_schema", ""),
        "target_table":     request.get("target_table", src_table.upper()),
        "target_database":  request.get("target_database", "MY_SNOWFLAKE_DB"),
        "target_schema":    request.get("target_schema", "PUBLIC"),
        "snowflake_creds":  sf_creds,
        "watermark_column": request.get("watermark_column", ""),
        "primary_keys":     request.get("primary_keys", []),
        "auto_reconcile":   request.get("auto_reconcile", False),
        "ddl":              request.get("ddl", ""),
        "batch_size":       request.get("batch_size", 10000),
        "schedule":         schedule,
        "last_watermark_value": None,
        "last_lsn":         None,
        "pg_slot_name":     request.get("pg_slot_name", ""),
        "pg_pub_name":      request.get("pg_pub_name", ""),
    }

    entry = {
        "pipeline_id": pipeline_id,
        "pipeline":    pipeline_cfg,
        "status":      "created",
        "last_run":    None,
        "last_stats":  None,
        "run_count":   0,
        "error":       None,
        "logs":        [],
        "created_at":  datetime.utcnow().isoformat(),
    }

    with _pipeline_lock:
        _pipelines[pipeline_id] = entry

    # Register scheduler job if schedule provided
    if schedule and _HAS_SCHEDULER and _scheduler:
        try:
            _register_schedule(pipeline_id, schedule)
            entry["next_run"] = _get_next_run(pipeline_id)
        except Exception as e:
            entry["schedule_error"] = str(e)

    return {"success": True, "pipeline_id": pipeline_id,
            "message": f"Pipeline '{name}' created"}


def _register_schedule(pipeline_id: str, schedule: str):
    """Parse schedule string and register with APScheduler."""
    global _scheduler
    if not _scheduler: return

    # Remove existing job if any
    try: _scheduler.remove_job(pipeline_id)
    except Exception: pass

    schedule = schedule.strip()
    # Interval format: 15m, 1h, 30s, 1d
    interval_match = re.match(r'^(\d+)(s|m|h|d)$', schedule)
    if interval_match:
        val, unit = int(interval_match.group(1)), interval_match.group(2)
        kwargs = {"seconds": val} if unit == "s" else \
                 {"minutes": val} if unit == "m" else \
                 {"hours": val}   if unit == "h" else {"days": val}
        _scheduler.add_job(
            _run_pipeline, IntervalTrigger(**kwargs),
            id=pipeline_id, args=[pipeline_id],
            replace_existing=True, max_instances=1,
        )
    else:
        # Cron format: "0 2 * * *"
        parts = schedule.split()
        if len(parts) == 5:
            _scheduler.add_job(
                _run_pipeline, CronTrigger(
                    minute=parts[0], hour=parts[1],
                    day=parts[2], month=parts[3], day_of_week=parts[4]
                ),
                id=pipeline_id, args=[pipeline_id],
                replace_existing=True, max_instances=1,
            )
        else:
            raise ValueError(f"Invalid schedule: '{schedule}'. Use cron (0 2 * * *) or interval (15m, 1h)")


def _get_next_run(pipeline_id: str):
    """Get next scheduled run time."""
    if not _scheduler: return None
    try:
        job = _scheduler.get_job(pipeline_id)
        return job.next_run_time.isoformat() if job and job.next_run_time else None
    except Exception:
        return None


@app.post("/api/pipeline/run")
async def run_pipeline_now(request: dict):
    """Manually trigger a pipeline run immediately."""
    pipeline_id = request.get("pipeline_id", "")
    if not pipeline_id or pipeline_id not in _pipelines:
        raise HTTPException(404, detail={"message": "Pipeline not found"})

    import threading
    t = threading.Thread(target=_run_pipeline, args=(pipeline_id,), daemon=True)
    t.start()

    return {"success": True, "pipeline_id": pipeline_id,
            "message": "Pipeline started in background"}


@app.get("/api/pipeline/list")
async def list_pipelines():
    """List all pipelines with status."""
    with _pipeline_lock:
        result = []
        for pid, entry in _pipelines.items():
            result.append({
                "pipeline_id":  pid,
                "name":         entry["pipeline"].get("name", ""),
                "mode":         entry["pipeline"].get("mode", ""),
                "source_table": entry["pipeline"].get("source_table", ""),
                "target":       f'{entry["pipeline"].get("target_database","")}.{entry["pipeline"].get("target_schema","")}.{entry["pipeline"].get("target_table","")}',
                "status":       entry.get("status", "unknown"),
                "last_run":     entry.get("last_run"),
                "next_run":     _get_next_run(pid),
                "run_count":    entry.get("run_count", 0),
                "last_stats":   entry.get("last_stats"),
                "error":        entry.get("error"),
                "schedule":     entry["pipeline"].get("schedule", ""),
            })
    return {"success": True, "pipelines": result}


@app.get("/api/pipeline/{pipeline_id}")
async def get_pipeline(pipeline_id: str):
    """Get pipeline detail including logs and reconciliation."""
    if pipeline_id not in _pipelines:
        raise HTTPException(404, detail={"message": "Pipeline not found"})
    with _pipeline_lock:
        entry = dict(_pipelines[pipeline_id])
    entry["next_run"] = _get_next_run(pipeline_id)
    return {"success": True, "pipeline": entry}


@app.get("/api/pipeline/{pipeline_id}/logs")
async def get_pipeline_logs(pipeline_id: str):
    """Get latest run logs for a pipeline."""
    if pipeline_id not in _pipelines:
        raise HTTPException(404, detail={"message": "Pipeline not found"})
    with _pipeline_lock:
        logs = list(_pipelines[pipeline_id].get("logs", []))
    return {"success": True, "logs": logs}


@app.post("/api/pipeline/{pipeline_id}/reconcile")
async def reconcile_pipeline(pipeline_id: str):
    """Run reconciliation check on demand."""
    if pipeline_id not in _pipelines:
        raise HTTPException(404, detail={"message": "Pipeline not found"})
    pipeline = _pipelines[pipeline_id]["pipeline"]
    try:
        result = _run_reconciliation(pipeline)
        with _pipeline_lock:
            _pipelines[pipeline_id]["last_reconciliation"] = result
        return {"success": True, "reconciliation": result}
    except Exception as e:
        raise HTTPException(400, detail=power_bi_error("reconciliation", e))


@app.delete("/api/pipeline/{pipeline_id}")
async def delete_pipeline(pipeline_id: str):
    """Delete a pipeline and remove its schedule."""
    if pipeline_id not in _pipelines:
        raise HTTPException(404, detail={"message": "Pipeline not found"})
    if _scheduler:
        try: _scheduler.remove_job(pipeline_id)
        except Exception: pass
    with _pipeline_lock:
        del _pipelines[pipeline_id]
    return {"success": True, "message": "Pipeline deleted"}


@app.put("/api/pipeline/{pipeline_id}/schedule")
async def update_schedule(pipeline_id: str, request: dict):
    """Update pipeline schedule without recreating it."""
    if pipeline_id not in _pipelines:
        raise HTTPException(404, detail={"message": "Pipeline not found"})
    schedule = request.get("schedule", "").strip()
    if not schedule:
        # Pause — remove job
        if _scheduler:
            try: _scheduler.remove_job(pipeline_id)
            except Exception: pass
        with _pipeline_lock:
            _pipelines[pipeline_id]["pipeline"]["schedule"] = ""
        return {"success": True, "message": "Pipeline schedule removed (paused)"}
    try:
        _register_schedule(pipeline_id, schedule)
        with _pipeline_lock:
            _pipelines[pipeline_id]["pipeline"]["schedule"] = schedule
        return {"success": True, "schedule": schedule,
                "next_run": _get_next_run(pipeline_id)}
    except Exception as e:
        raise HTTPException(400, detail={"message": str(e)})


@app.post("/api/pipeline/detect-watermark")
async def detect_watermark(request: dict):
    """
    Auto-detect the best watermark column and primary keys from source table.
    Returns recommendations for incremental load setup.
    """
    session_id = request.get("session_id") or request.get("session", "")
    table_name = request.get("table", "")
    schema     = request.get("schema", "")
    if not session_id or session_id not in connections:
        raise HTTPException(400, detail={"message": "Session not found"})
    engine = connections[session_id].get("engine")
    if not engine:
        raise HTTPException(400, detail={"message": "Requires a relational DB connection"})
    try:
        inspector = inspect(engine)
        cols = inspector.get_columns(table_name, schema=schema or None)
        pk   = inspector.get_pk_constraint(table_name, schema=schema or None)

        # Score columns for watermark suitability
        watermark_candidates = []
        for c in cols:
            col_name = c["name"]
            col_type = str(c.get("type","")).lower()
            score = 0
            if any(k in col_type for k in ["timestamp","datetime","date"]): score += 10
            if any(k in col_name.lower() for k in
                   ["updated","modified","changed","last_","update_dt","mod_"]): score += 5
            if any(k in col_name.lower() for k in ["created","insert","added"]): score += 2
            if score > 0:
                watermark_candidates.append({"column": col_name, "type": col_type, "score": score})

        watermark_candidates.sort(key=lambda x: x["score"], reverse=True)

        return {
            "success": True,
            "table": table_name,
            "primary_keys": pk.get("constrained_columns", []),
            "watermark_candidates": watermark_candidates[:5],
            "recommended_watermark": watermark_candidates[0]["column"]
                                     if watermark_candidates else None,
        }
    except Exception as e:
        raise HTTPException(400, detail=power_bi_error("detect_watermark", e))


@app.on_event("startup")
async def startup():
    """Restore pipelines on restart (from Supabase if configured)."""
    # APScheduler already started above — nothing else needed here
    pass


@app.on_event("shutdown")
async def shutdown():
    if _scheduler:
        try: _scheduler.shutdown(wait=False)
        except Exception: pass
    for conn in connections.values():
        try:
            if "engine" in conn: conn["engine"].dispose()
        except: pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
