"""
Data Cleaner — Optimised for large files (100MB+)
==================================================
Key optimisations vs previous version:
  1.  @st.cache_data on file load and cleaning pipeline — never recomputed on
      widget re-renders; keyed by SHA-256 of the uploaded file bytes.
  2.  Vectorised string trim via Series.str.strip() (C-level) — 50x faster
      than apply(lambda x: x.strip()).
  3.  Date detection: compiled regex + vectorised str.match() on a 200-row
      sample only. O(1) per column instead of O(N × P).
  4.  Nullish-string replacement: single vectorised isin() mask, no loops.
  5.  Outlier detection: numpy matrix z-score across ALL numeric columns in
      one operation — one pass, no per-column Python loops.
  6.  Removed-rows tracking: boolean index masks accumulated on original index;
      single .loc[] slice at the end — zero intermediate DataFrame copies.
  7.  Duplicate detection: keep='first' with no redundant copy().
  8.  Schema preview: uses head(1) sample value — no full-column scan per render.
  9.  DDL VARCHAR length inference: 500-row sample, not full column.
 10.  CSV loader: low_memory=False avoids multi-pass dtype guessing.
 11.  Parquet loader: pyarrow.parquet.read_table() direct — fastest path.
 12.  Export: @st.cache_data — converting the same df to the same format costs
      0ms on second click.
 13.  Export triggered by explicit button click, not by radio-change re-render.
"""

import streamlit as st
import pandas as pd
import numpy as np
import json
import io
import re
import hashlib
import datetime as dt
import requests

# ══════════════════════════════════════════════════════════════════════════════
#  SUPABASE AUTH GUARD — blocks access unless user is logged in
#  The website passes ?token=xxx in the URL after login.
#  This verifies the token with Supabase before showing the app.
# ══════════════════════════════════════════════════════════════════════════════

SUPABASE_URL = "https://yxwlgwaalghvskulhmey.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl4d2xnd2FhbGdodnNrdWxobWV5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzg4ODEwODYsImV4cCI6MjA5NDQ1NzA4Nn0.QOZphIHDIebIkSX23LPjQXtr-iICT9dapzYMw11HBTw"

def verify_supabase_token(token):
    """Verify a Supabase JWT token by calling Supabase auth API."""
    try:
        resp = requests.get(
            f"{SUPABASE_URL}/auth/v1/user",
            headers={
                "Authorization": f"Bearer {token}",
                "apikey": SUPABASE_KEY,
            },
            timeout=5
        )
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception:
        return None

def check_auth():
    """
    Check if user is authenticated.
    Accepts token from:
      1. URL query parameter: ?token=xxx
      2. Session state (after first verification)
    """
    # Already verified this session
    if st.session_state.get("authenticated"):
        return st.session_state.get("user_info")

    # Check URL query parameter
    params = st.query_params
    token = params.get("token", None)

    if token:
        user = verify_supabase_token(token)
        if user:
            st.session_state["authenticated"] = True
            st.session_state["user_info"] = user
            st.session_state["user_email"] = user.get("email", "")
            st.session_state["user_name"] = user.get("user_metadata", {}).get("full_name", "")
            return user

    return None

# ── Run auth check ────────────────────────────────────────────────────────────
user = check_auth()

if not user:
    st.set_page_config(page_title="Migranix — Login Required", page_icon="🔒", layout="centered")
    st.markdown("""
    <style>
    .login-box {
        max-width: 440px; margin: 80px auto; text-align: center;
        background: #fff; border-radius: 16px; padding: 48px 40px;
        box-shadow: 0 4px 24px rgba(0,0,0,.08); border: 1px solid #e5e7eb;
    }
    .login-logo { font-size: 1.3rem; font-weight: 700; color: #0f172a; margin-bottom: 24px; }
    .login-logo span { display: inline-block; width: 10px; height: 10px;
        background: #2563eb; border-radius: 50%; margin-right: 8px; }
    .login-title { font-size: 1.5rem; font-weight: 700; color: #0f172a; margin-bottom: 8px; }
    .login-sub { color: #6b7280; font-size: .9rem; margin-bottom: 28px; line-height: 1.6; }
    .login-btn {
        display: inline-block; background: #2563eb; color: #fff;
        padding: 14px 36px; border-radius: 10px; font-weight: 700;
        font-size: 1rem; text-decoration: none; transition: all .15s;
    }
    .login-btn:hover { background: #1d4ed8; transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(37,99,235,.3); }
    .login-note { color: #9ca3af; font-size: .78rem; margin-top: 20px; }
    </style>
    <div class="login-box">
        <div class="login-logo"><span></span>Migranix</div>
        <div class="login-title">🔒 Login Required</div>
        <div class="login-sub">
            You need to log in to access the Migranix Data Cleaning platform.<br>
            Sign up or log in on our website, then you'll be redirected here automatically.
        </div>
        <a class="login-btn" href="https://www.migranix.in">Go to migranix.in →</a>
        <div class="login-note">Free 14-day trial · No credit card required</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()


# ─── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="DataFlow", page_icon="🧹",
                   layout="wide", initial_sidebar_state="expanded")

# ─── Professional White Theme CSS ────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

/* ── Global ── */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif !important;
    color: #1a1d23;
}
.stApp { background: #f5f6fa; }
code, pre, .stCodeBlock { font-family: 'JetBrains Mono', monospace !important; }

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #ffffff;
    border-right: 1px solid #e5e7eb;
    box-shadow: 2px 0 8px rgba(0,0,0,.04);
}
section[data-testid="stSidebar"] > div { padding-top: 1.5rem; }

/* ── Main content card areas ── */
.block-container {
    padding: 2rem 2.5rem 3rem 2.5rem !important;
    max-width: 1280px;
}

/* ── Headers ── */
h1 { color: #0f172a !important; font-weight: 700 !important; font-size: 1.75rem !important; letter-spacing: -.02em; }
h2 { color: #0f172a !important; font-weight: 600 !important; font-size: 1.2rem !important;
     margin-top: 0.5rem !important; padding-bottom: 0.4rem;
     border-bottom: 2px solid #e5e7eb; }
h3 { color: #374151 !important; font-weight: 600 !important; font-size: 1rem !important; }

/* ── Section cards ── */
.section-card {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 12px;
    padding: 1.5rem 1.75rem;
    margin-bottom: 1.25rem;
    box-shadow: 0 1px 4px rgba(0,0,0,.04);
}

/* ── Metrics ── */
[data-testid="metric-container"] {
    background: #ffffff;
    border: 1px solid #e5e7eb;
    border-radius: 10px;
    padding: 1rem 1.25rem;
    box-shadow: 0 1px 3px rgba(0,0,0,.05);
}
[data-testid="metric-container"] label {
    color: #6b7280 !important;
    font-size: .72rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: .08em;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #0f172a !important;
    font-family: 'Inter', sans-serif !important;
    font-size: 1.65rem !important;
    font-weight: 700 !important;
}
[data-testid="metric-container"] [data-testid="stMetricDelta"] { font-size: .78rem; }

/* ── Primary buttons ── */
.stButton > button {
    background: #2563eb;
    color: #ffffff;
    border: none;
    border-radius: 8px;
    font-weight: 600;
    font-size: .875rem;
    padding: .55rem 1.4rem;
    letter-spacing: .01em;
    transition: all .18s ease;
    box-shadow: 0 1px 3px rgba(37,99,235,.25);
}
.stButton > button:hover {
    background: #1d4ed8;
    box-shadow: 0 4px 12px rgba(37,99,235,.35);
    transform: translateY(-1px);
}
.stButton > button:active { transform: translateY(0); }

/* ── Download buttons ── */
.stDownloadButton > button {
    background: #ffffff;
    color: #2563eb;
    border: 1.5px solid #2563eb;
    border-radius: 8px;
    font-weight: 600;
    font-size: .875rem;
    transition: all .18s;
}
.stDownloadButton > button:hover {
    background: #eff6ff;
    box-shadow: 0 3px 10px rgba(37,99,235,.15);
}

/* ── File uploader ── */
[data-testid="stFileUploader"] {
    background: #ffffff;
    border: 2px dashed #d1d5db;
    border-radius: 10px;
    padding: 1.5rem;
    transition: border-color .2s;
}
[data-testid="stFileUploader"]:hover { border-color: #2563eb; }

/* ── Select / text inputs ── */
.stSelectbox > div > div,
.stTextInput > div > div > input,
.stTextArea > div > div > textarea {
    background: #ffffff !important;
    border: 1.5px solid #d1d5db !important;
    border-radius: 8px !important;
    color: #1a1d23 !important;
    font-size: .875rem !important;
    transition: border-color .18s;
}
.stTextInput > div > div > input:focus,
.stTextArea > div > div > textarea:focus { border-color: #2563eb !important; box-shadow: 0 0 0 3px rgba(37,99,235,.12) !important; }

/* ── Radio buttons ── */
.stRadio > div { gap: .6rem; }
.stRadio > div > label {
    background: #f9fafb;
    border: 1.5px solid #e5e7eb;
    border-radius: 8px;
    padding: .45rem .9rem;
    font-size: .855rem;
    font-weight: 500;
    cursor: pointer;
    transition: all .15s;
}
.stRadio > div > label:hover { border-color: #2563eb; background: #eff6ff; }

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    gap: .25rem;
    background: #f1f5f9;
    border-radius: 10px;
    padding: .3rem;
    border: none;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 8px;
    font-weight: 500;
    font-size: .875rem;
    color: #6b7280;
    padding: .45rem 1rem;
    border: none;
    background: transparent;
}
.stTabs [aria-selected="true"] {
    background: #ffffff !important;
    color: #2563eb !important;
    font-weight: 600;
    box-shadow: 0 1px 4px rgba(0,0,0,.08);
}

/* ── Expanders ── */
.streamlit-expanderHeader {
    background: #f9fafb !important;
    border: 1px solid #e5e7eb !important;
    border-radius: 8px !important;
    font-weight: 500 !important;
    font-size: .875rem !important;
    color: #374151 !important;
}
.streamlit-expanderContent {
    border: 1px solid #e5e7eb !important;
    border-top: none !important;
    border-radius: 0 0 8px 8px !important;
    background: #ffffff !important;
}

/* ── Alerts ── */
.stSuccess {
    background: #f0fdf4 !important;
    border: 1px solid #bbf7d0 !important;
    border-left: 4px solid #16a34a !important;
    border-radius: 8px !important;
    color: #166534 !important;
}
.stWarning {
    background: #fffbeb !important;
    border: 1px solid #fde68a !important;
    border-left: 4px solid #d97706 !important;
    border-radius: 8px !important;
    color: #92400e !important;
}
.stError {
    background: #fef2f2 !important;
    border: 1px solid #fecaca !important;
    border-left: 4px solid #dc2626 !important;
    border-radius: 8px !important;
    color: #991b1b !important;
}
.stInfo {
    background: #eff6ff !important;
    border: 1px solid #bfdbfe !important;
    border-left: 4px solid #2563eb !important;
    border-radius: 8px !important;
    color: #1e40af !important;
}

/* ── DataFrames ── */
.stDataFrame {
    border: 1px solid #e5e7eb;
    border-radius: 10px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,.05);
}
.stDataFrame thead th {
    background: #f8fafc !important;
    font-weight: 600 !important;
    color: #374151 !important;
    font-size: .8rem !important;
    text-transform: uppercase;
    letter-spacing: .05em;
}

/* ── Code blocks ── */
.stCodeBlock {
    background: #1e293b !important;
    border-radius: 8px !important;
    border: none !important;
}

/* ── Divider ── */
hr { border-color: #e5e7eb !important; margin: 1.5rem 0 !important; }

/* ── Badges ── */
.badge {
    display: inline-flex; align-items: center;
    padding: 3px 10px;
    border-radius: 20px;
    font-size: .72rem;
    font-weight: 600;
    font-family: 'Inter', sans-serif;
    letter-spacing: .03em;
    margin: 2px;
}
.badge-red    { background: #fef2f2; color: #dc2626; border: 1px solid #fca5a5; }
.badge-green  { background: #f0fdf4; color: #16a34a; border: 1px solid #86efac; }
.badge-blue   { background: #eff6ff; color: #2563eb; border: 1px solid #93c5fd; }
.badge-yellow { background: #fffbeb; color: #d97706; border: 1px solid #fcd34d; }
.badge-gray   { background: #f9fafb; color: #6b7280; border: 1px solid #d1d5db; }

/* ── Checklist items in sidebar ── */
.step-item {
    display: flex;
    align-items: flex-start;
    gap: .55rem;
    padding: .55rem .75rem;
    border-radius: 8px;
    margin-bottom: .3rem;
    font-size: .83rem;
    font-weight: 500;
    transition: background .15s;
}
.step-done {
    background: #f0fdf4;
    color: #15803d;
    border: 1px solid #bbf7d0;
}
.step-active {
    background: #eff6ff;
    color: #1d4ed8;
    border: 1px solid #bfdbfe;
}
.step-pending {
    background: #f9fafb;
    color: #9ca3af;
    border: 1px solid #f3f4f6;
}
.step-icon { font-size: .95rem; line-height: 1.2; flex-shrink: 0; }
.step-label { line-height: 1.3; }
.step-sub { font-size: .73rem; font-weight: 400; margin-top: .1rem; opacity: .75; }

/* ── Page header banner ── */
.header-banner {
    background: linear-gradient(135deg, #1e40af 0%, #2563eb 50%, #3b82f6 100%);
    border-radius: 14px;
    padding: 1.5rem 2rem;
    margin-bottom: 1.75rem;
    color: #fff;
    display: flex;
    align-items: center;
    gap: 1.25rem;
    box-shadow: 0 4px 20px rgba(37,99,235,.2);
}
.header-icon { font-size: 2.2rem; }
.header-title { font-size: 1.5rem; font-weight: 700; letter-spacing: -.02em; }
.header-sub { font-size: .875rem; opacity: .85; margin-top: .2rem; font-weight: 400; }

/* ── Section header pills ── */
.section-pill {
    display: inline-flex; align-items: center; gap: .4rem;
    background: #eff6ff;
    color: #1d4ed8;
    border: 1px solid #bfdbfe;
    border-radius: 20px;
    padding: .3rem .9rem;
    font-size: .78rem;
    font-weight: 600;
    letter-spacing: .04em;
    text-transform: uppercase;
    margin-bottom: .6rem;
}

/* ── Spinner ── */
.stSpinner > div { border-top-color: #2563eb !important; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def file_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


def _step_html(icon: str, label: str, sub: str, state: str) -> str:
    """Render a single checklist step. state: done | active | pending"""
    css = f"step-item step-{state}"
    check = "✅" if state == "done" else ("🔵" if state == "active" else "⬜")
    return (
        f'<div class="{css}">'
        f'<span class="step-icon">{check}</span>'
        f'<div class="step-label">{icon} {label}'
        f'<div class="step-sub">{sub}</div></div></div>'
    )


def _section_pill(icon: str, label: str) -> None:
    st.markdown(
        f'<div class="section-pill">{icon} {label}</div>',
        unsafe_allow_html=True)



# ══════════════════════════════════════════════════════════════════════════════
#  SNOWFLAKE MODULE
# ══════════════════════════════════════════════════════════════════════════════

def pd_dtype_to_snowflake(dtype_str: str, sample: pd.Series = None) -> str:
    d = dtype_str.lower()
    if d in ("int64","int32","int16","int8"): return "NUMBER(38,0)"
    if d in ("float64","float32"):
        if sample is not None:
            nn = sample.dropna().iloc[:500]
            if len(nn) > 0:
                try:
                    if (nn == nn.astype("int64")).all(): return "NUMBER(38,0)"
                except Exception: pass
        return "FLOAT"
    if d == "bool":             return "BOOLEAN"
    if d.startswith("datetime64"): return "TIMESTAMP_NTZ"
    if d == "date":             return "DATE"
    if d.startswith("timedelta"): return "VARCHAR(50)"
    if d == "object":
        if sample is not None:
            ml = sample.dropna().iloc[:500].astype(str).str.len().max()
            if pd.notna(ml):
                ml = int(ml)
                if ml <= 50:   return "VARCHAR(100)"
                if ml <= 255:  return "VARCHAR(500)"
                if ml <= 1000: return "VARCHAR(4000)"
        return "VARCHAR(16777216)"
    return "VARCHAR(16777216)"


def build_create_table_sql(table_name: str, df: pd.DataFrame) -> str:
    lines = []
    for i, col in enumerate(df.columns):
        safe = re.sub(r"[^a-zA-Z0-9_]","_",str(col)).strip("_") or f"col_{i}"
        lines.append(f'    "{safe.upper()}"  {pd_dtype_to_snowflake(str(df[col].dtype), df[col])}')
    return f'CREATE OR REPLACE TABLE {table_name} (\n' + ",\n".join(lines) + "\n);"


def get_snowflake_connection(creds: dict):
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            account=creds["account"], user=creds["user"], password=creds["password"],
            warehouse=creds["warehouse"], database=creds["database"],
            schema=creds["schema"], role=creds.get("role",""))
        return conn, None
    except ImportError:
        return None, "snowflake-connector-python not installed. Run: pip install snowflake-connector-python"
    except Exception as e:
        return None, str(e)


def table_exists_in_snowflake(conn, database, schema, table_name):
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {database}.INFORMATION_SCHEMA.TABLES "
                    f"WHERE TABLE_SCHEMA='{schema.upper()}' AND TABLE_NAME='{table_name.upper()}'")
        n = cur.fetchone()[0]; cur.close(); return n > 0
    except Exception: return False


def get_existing_table_columns(conn, database, schema, table_name):
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE "
                    f"FROM {database}.INFORMATION_SCHEMA.COLUMNS "
                    f"WHERE TABLE_SCHEMA='{schema.upper()}' AND TABLE_NAME='{table_name.upper()}' "
                    f"ORDER BY ORDINAL_POSITION")
        rows = cur.fetchall(); cur.close()
        return [{"name":r[0],"type":r[1],"max_length":r[2],"precision":r[3],"scale":r[4]} for r in rows]
    except Exception: return []


def load_df_to_snowflake(conn, table_name, df, table_exists, load_mode):
    result = {"success":False,"rows_loaded":0,"ddl":None,"errors":[]}
    cur = conn.cursor()
    try:
        if not table_exists or load_mode == "create":
            ddl = build_create_table_sql(table_name, df)
            result["ddl"] = ddl; cur.execute(ddl)
        if table_exists and load_mode == "overwrite":
            cur.execute(f"TRUNCATE TABLE {table_name};")
        try:
            from snowflake.connector.pandas_tools import write_pandas
            safe = df.copy()
            safe.columns = [re.sub(r"[^a-zA-Z0-9_]","_",str(c)).strip("_").upper() for c in safe.columns]
            for col in safe.columns:
                s = safe[col].dropna()
                if len(s)>0 and isinstance(s.iloc[0], dt.date):
                    safe[col] = safe[col].astype(str)
            tbl_only = table_name.split(".")[-1].strip('"')
            ok,_,nrows,_ = write_pandas(conn, safe, tbl_only, auto_create_table=False, overwrite=False)
            result["success"]=ok; result["rows_loaded"]=nrows
        except Exception as e:
            result["errors"].append(f"write_pandas failed ({e}), using INSERT fallback.")
            safe2 = df.copy()
            safe2.columns = [re.sub(r"[^a-zA-Z0-9_]","_",str(c)).strip("_").upper() for c in safe2.columns]
            cols_str = ", ".join(f'"{c}"' for c in safe2.columns)
            for start in range(0, len(safe2), 500):
                chunk = safe2.iloc[start:start+500]
                rows = []
                for tup in chunk.itertuples(index=False):
                    vals = []
                    for v in tup:
                        if v is None or (isinstance(v,float) and np.isnan(v)):
                            vals.append("NULL")
                        elif isinstance(v, bool): vals.append("TRUE" if v else "FALSE")
                        elif isinstance(v,(int,float)): vals.append(str(v))
                        elif isinstance(v,(pd.Timestamp,dt.datetime)):
                            vals.append(f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'")
                        else: vals.append(f"'{str(v).replace(chr(39),chr(39)*2)}'")
                    rows.append(f"({','.join(vals)})")
                cur.execute(f"INSERT INTO {table_name} ({cols_str}) VALUES {','.join(rows)};")
            result["success"]=True; result["rows_loaded"]=len(df)
    except Exception as e:
        result["errors"].append(str(e))
    finally:
        cur.close()
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  OPTIMISED CLEANING ENGINE
# ══════════════════════════════════════════════════════════════════════════════

# Compiled once at module load — not per-call
_DATE_RE     = re.compile(r"^\d{4}-\d{2}-\d{2}$|^\d{2}/\d{2}/\d{4}$|^\d{2}-\d{2}-\d{4}$|^\d{4}/\d{2}/\d{2}$")
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}|^\d{2}/\d{2}/\d{4} \d{2}:\d{2}|^\d{2}-\d{2}-\d{4} \d{2}:\d{2}")
_NULL_SET    = frozenset({"na","n/a","null","none","nan","-","","undefined","nil","#n/a","#na"})
_BOOL_MAP    = {"true":True,"false":False,"yes":True,"no":False,"1":True,"0":False,"t":True,"f":False}


def _detect_date_kind(series: pd.Series) -> str | None:
    """Vectorised date detection on a 200-row sample. O(1) not O(N)."""
    sample = series.dropna().astype(str).str.strip().head(200)
    if len(sample) == 0: return None
    n = len(sample)
    if sample.str.match(_DATETIME_RE).sum() / n > 0.6: return "datetime"
    if sample.str.match(_DATE_RE).sum()     / n > 0.6: return "date"
    return None


def run_cleaning_pipeline(original_df: pd.DataFrame) -> dict:
    """
    Full vectorised cleaning pipeline.
    Accepts DataFrame directly — zero JSON serialisation overhead.
    Result stored in session_state by fhash — 0ms on repeated renders.
    Returns dict with: cleaned_df, removed_df, date_conv, orig_shape, steps.
    """
    df   = original_df.copy()
    steps: list[str]        = []
    dc   : list[tuple]      = []
    orig : tuple            = df.shape

    # Each entry: (index_array, short_label, detail_func)
    # detail_func(row) → human-readable explanation string for that specific row
    removed_idx: list[tuple] = []

    # 1. Trim column names — vectorised
    df.columns = df.columns.str.strip()
    steps.append("Trimmed column header whitespace")

    # 2. Vectorised cell trim — C-level str.strip(), ~50× faster than apply(lambda)
    str_cols = df.select_dtypes(include="object").columns.tolist()
    trimmed  = []
    for col in str_cols:
        stripped = df[col].str.strip()
        if not stripped.equals(df[col]):
            df[col] = stripped; trimmed.append(col)
    if trimmed:
        steps.append(f"Trimmed whitespace in {len(trimmed)} string column(s)")

    # 3. Nullish strings → NaN — single vectorised isin() per column
    for col in df.select_dtypes(include="object").columns:
        mask = df[col].str.strip().str.lower().isin(_NULL_SET)
        if mask.any():
            df.loc[mask, col] = np.nan
            steps.append(f"'{col}': {int(mask.sum())} nullish strings → NaN")

    # 4. Remove fully-empty rows — detailed per-row reason
    null_mask = df.isnull().all(axis=1)
    if null_mask.any():
        null_idx = df.index[null_mask].to_numpy()

        def _null_reason(row):
            total = len(row)
            return (
                f"Row removed: all {total} column(s) are empty/null. "
                f"Columns: {', '.join(str(c) for c in row.index.tolist()[:8])}"
                f"{'…' if total > 8 else ''}. "
                f"No usable data present in this row."
            )

        removed_idx.append((null_idx, "Data missing — all columns null", _null_reason))
        df = df.loc[~null_mask]
        steps.append(f"Removed {int(null_mask.sum())} fully-empty row(s)")

    # 5. Remove duplicates — pandas C-level hash
    dup_mask = df.duplicated(keep="first")
    if dup_mask.any():
        dup_idx = df.index[dup_mask].to_numpy()

        def _dup_reason(row):
            key_cols = row.index.tolist()[:5]
            key_vals = [f"{c}='{str(row[c])[:30]}'" for c in key_cols]
            return (
                f"Row removed: exact duplicate of an earlier row. "
                f"Key values — {', '.join(key_vals)}"
                f"{'…' if len(row) > 5 else ''}. "
                f"Only the first occurrence is retained."
            )

        removed_idx.append((dup_idx, "Duplicate row — identical to an earlier record", _dup_reason))
        df = df.loc[~dup_mask]
        steps.append(f"Removed {int(dup_mask.sum())} duplicate row(s)")

    # 6. Numeric inference — pd.to_numeric is C-level
    for col in df.select_dtypes(include="object").columns:
        conv = pd.to_numeric(df[col], errors="coerce")
        nn_o = df[col].notna().sum(); nn_c = conv.notna().sum()
        if nn_o > 0 and nn_c / nn_o >= 0.85:
            df[col] = conv
            steps.append(f"'{col}': text → numeric")

    # 7. Boolean inference — vectorised map
    for col in df.select_dtypes(include="object").columns:
        lower = df[col].dropna().astype(str).str.lower()
        if len(lower) > 0 and set(lower.unique()).issubset(_BOOL_MAP):
            df[col] = df[col].astype(str).str.lower().map(_BOOL_MAP)
            steps.append(f"'{col}': text → boolean")

    # 8. Date / datetime — sampled detection (200-row sample only)
    for col in df.select_dtypes(include="object").columns:
        kind = _detect_date_kind(df[col])
        if kind == "datetime":
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                dc.append((col,"datetime64[ns]"))
                steps.append(f"'{col}': text → datetime")
            except Exception: pass
        elif kind == "date":
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
                dc.append((col,"date"))
                steps.append(f"'{col}': text → date")
            except Exception: pass

    # 9. Outlier detection — single numpy matrix pass across ALL numeric cols
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    out_z_scores: dict = {}   # col → z-score array for detail comments
    if num_cols:
        mat   = df[num_cols].to_numpy(dtype=float, na_value=np.nan)
        means = np.nanmean(mat, axis=0)
        stds  = np.nanstd(mat,  axis=0) + 1e-9
        z_mat = np.abs((mat - means) / stds)
        out_rows = np.any(z_mat > 5, axis=1)

        # Pre-compute per-outlier-row which columns triggered the flag
        if out_rows.sum() > 0:
            out_row_indices = np.where(out_rows)[0]

            # Store z-matrix slice for the outlier rows
            out_z_mat   = z_mat[out_rows]          # shape (n_out, n_num_cols)
            out_raw_mat = mat[out_rows]             # actual values
            out_df_idx  = df.index[out_rows].to_numpy()

            # Build a lookup: df_index → list of (col, value, z_score)
            outlier_details: dict = {}
            for i, df_i in enumerate(out_df_idx):
                triggers = []
                for j, col in enumerate(num_cols):
                    z = float(out_z_mat[i, j])
                    if z > 5:
                        triggers.append((col, float(out_raw_mat[i, j]), z))
                outlier_details[df_i] = triggers

            def _make_outlier_reason(details_map):
                def _outlier_reason(row):
                    idx = row.name
                    triggers = details_map.get(idx, [])
                    if triggers:
                        parts = [f"'{c}' = {v:.4g} (z-score = {z:.2f})" for c, v, z in triggers]
                        return (
                            f"Row removed: statistical outlier. "
                            f"The following column(s) have extreme values (|z-score| > 5): "
                            f"{'; '.join(parts)}. "
                            f"These values are more than 5 standard deviations from the column mean."
                        )
                    return "Row removed: statistical outlier (extreme z-score > 5 detected)."
                return _outlier_reason

            removed_idx.append((
                out_df_idx,
                "Statistical outlier — extreme value (|z-score| > 5)",
                _make_outlier_reason(outlier_details)
            ))
            df = df.loc[~out_rows]
            steps.append(f"Removed {int(out_rows.sum())} extreme outlier row(s)")

    # ── Build removed_df with detailed 'reason' column ────────────────────────
    frames = []
    for idx_arr, short_label, detail_fn in removed_idx:
        valid = original_df.index.intersection(idx_arr)
        if len(valid):
            sub = original_df.loc[valid].copy()
            # Generate a detailed per-row reason comment
            sub["reason"] = sub.apply(detail_fn, axis=1)
            frames.append(sub)

    removed_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # Ensure 'reason' is the LAST column for readability
    if not removed_df.empty and "reason" in removed_df.columns:
        cols_order = [c for c in removed_df.columns if c != "reason"] + ["reason"]
        removed_df = removed_df[cols_order]

    return {
        "cleaned_df":  df.reset_index(drop=True),
        "removed_df":  removed_df,
        "date_conv":   dc,
        "orig_shape":  orig,
        "steps":       steps,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  LATERAL FLATTEN ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def _flatten_record(record: dict, prefix="", sep="_") -> dict:
    out = {}
    for k, v in record.items():
        fk = f"{prefix}{sep}{k}" if prefix else k
        if isinstance(v, dict):
            out.update(_flatten_record(v, prefix=fk, sep=sep))
        elif isinstance(v, list):
            if not v: out[fk] = None
            elif all(isinstance(i, dict) for i in v): out[fk] = v
            else: out[fk] = ", ".join(str(i) for i in v)
        else:
            out[fk] = v
    return out


def _lateral_flatten_records(records: list, sep="_") -> list[dict]:
    result = []
    for record in records:
        flat = _flatten_record(record, sep=sep)
        acols = {k:v for k,v in flat.items() if isinstance(v,list)}
        scols = {k:v for k,v in flat.items() if not isinstance(v,list)}
        if not acols:
            result.append(scols); continue
        exploded = [scols.copy()]
        for ak, av in acols.items():
            nxt = []
            for pr in exploded:
                for ci in av:
                    if isinstance(ci, dict):
                        nxt.append({**pr, **{f"{ak}{sep}{ck}":cv for ck,cv in ci.items()}})
                    else:
                        nxt.append({**pr, ak: ci})
            exploded = nxt if nxt else exploded
        result.extend(_lateral_flatten_records(exploded, sep=sep))
    return result


def flatten_json_to_df(raw, sep="_") -> pd.DataFrame:
    def _post(df):
        for c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].apply(lambda x: json.dumps(x,default=str) if isinstance(x,(list,dict)) else x)
        return df

    if isinstance(raw, list):
        if not raw: return pd.DataFrame()
        if all(not isinstance(i,dict) for i in raw): return pd.DataFrame({"value":raw})
        return _post(pd.DataFrame(_lateral_flatten_records(raw, sep=sep)))

    if isinstance(raw, dict):
        lv = {k:v for k,v in raw.items() if isinstance(v,list)}
        nl = {k:v for k,v in raw.items() if not isinstance(v,list)}
        if lv:
            lens = [len(v) for v in lv.values()]
            if len(set(lens))==1 and all(not isinstance(i,dict) for s in lv.values() for i in s):
                df = pd.DataFrame(lv)
                for k,v in nl.items(): df[k]=v
                return df
        sc, da = {}, {}
        for k, v in raw.items():
            if not isinstance(v,(list,dict)): sc[k]=v
            elif isinstance(v,dict):
                for sk,sv in _flatten_record(v,prefix=k,sep=sep).items(): sc[sk]=sv
            elif isinstance(v,list):
                if not v: sc[k]=None
                elif all(not isinstance(i,dict) for i in v): sc[k]=", ".join(str(i) for i in v)
                elif all(isinstance(i,dict) and len(i)==1 for i in v):
                    sc[k]=", ".join(str(list(i.values())[0]) for i in v)
                else: da[k]=v
        if not da: return pd.DataFrame([sc])
        frames=[]
        for ak,av in da.items():
            frames.append(pd.DataFrame(_lateral_flatten_records([{**sc,**r} for r in av], sep=sep)))
        df = pd.concat(frames,ignore_index=True) if len(frames)>1 else frames[0]
        return _post(df)

    return pd.DataFrame({"value":[raw]})


def parse_ndjson(data: bytes) -> pd.DataFrame | None:
    try:
        records = [json.loads(l) for l in data.decode("utf-8").strip().splitlines() if l.strip()]
        if records:
            flat = _lateral_flatten_records(records, sep="_")
            return pd.DataFrame(flat) if flat else None
    except Exception: pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  FILE LOADER  — cached by (file_bytes, ext)
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner=False)
def load_file_cached(data: bytes, ext: str, fname: str):
    """Returns (df, info_message). Cached — same file costs 0ms second time."""
    msg = ""
    if ext == "csv":
        try:
            import csv as csv_mod
            dialect = csv_mod.Sniffer().sniff(data[:4096].decode("utf-8",errors="replace"), delimiters=",;\t|")
            sep = dialect.delimiter
        except Exception: sep = ","
        return pd.read_csv(io.BytesIO(data), sep=sep, low_memory=False, encoding_errors="replace"), msg

    if ext == "tsv":
        return pd.read_csv(io.BytesIO(data), sep="\t", low_memory=False, encoding_errors="replace"), msg

    if ext in ("xls","xlsx","xlsm","xlsb"):
        xf = pd.ExcelFile(io.BytesIO(data))
        if len(xf.sheet_names)==1:
            return pd.read_excel(io.BytesIO(data), engine="openpyxl"), msg
        msg = f"Excel: {len(xf.sheet_names)} sheets concatenated with __sheet__ column."
        frames = []
        for sh in xf.sheet_names:
            d = pd.read_excel(io.BytesIO(data), sheet_name=sh, engine="openpyxl")
            d["__sheet__"]=sh; frames.append(d)
        return pd.concat(frames,ignore_index=True), msg

    if ext == "json":
        text = data.decode("utf-8",errors="replace").strip()
        if text.startswith("{") and "\n" in text:
            ndf = parse_ndjson(data)
            if ndf is not None and len(ndf)>1:
                return ndf, "NDJSON detected and lateral-flattened."
        try: raw = json.loads(text)
        except json.JSONDecodeError as e: return None, f"JSON parse error: {e}"
        try:
            df = flatten_json_to_df(raw, sep="_")
            if df.empty: return None, "JSON parsed but empty DataFrame."
            if isinstance(raw,list): msg=f"JSON array {len(raw)} records → {len(df)} rows × {len(df.columns)} cols (lateral flattened)"
            elif isinstance(raw,dict):
                lk=[k for k,v in raw.items() if isinstance(v,list)]
                msg=f"JSON object (array keys: {lk}) → {len(df)} rows × {len(df.columns)} cols"
            return df, msg
        except Exception as e: return None, f"JSON flatten error: {e}"

    if ext == "parquet":
        try:
            import pyarrow.parquet as pq
            return pq.read_table(io.BytesIO(data)).to_pandas(), msg
        except Exception as e: return None, f"Parquet error: {e}"

    if ext == "avro":
        try:
            import fastavro
            recs = list(fastavro.reader(io.BytesIO(data)))
            if not recs: return pd.DataFrame(), "Avro empty."
            return pd.DataFrame(_lateral_flatten_records(recs, sep="_")), msg
        except ImportError: return None, "pip install fastavro"
        except Exception as e: return None, f"Avro error: {e}"

    if ext == "orc":
        try:
            import pyarrow.orc as orc
            return orc.read_table(io.BytesIO(data)).to_pandas(), msg
        except ImportError: return None, "pip install pyarrow"
        except Exception as e: return None, f"ORC error: {e}"

    if ext == "xml":
        try: return pd.read_xml(io.BytesIO(data)), msg
        except Exception:
            try:
                from lxml import etree
                root = etree.parse(io.BytesIO(data)).getroot()
                recs=[]
                for child in root:
                    rec=dict(child.attrib)
                    for el in child:
                        tag=el.tag.split("}")[-1] if "}" in el.tag else el.tag
                        rec[tag]=el.text
                    recs.append(rec)
                return (pd.DataFrame(recs) if recs else None), msg
            except Exception as e: return None, f"XML error: {e}"

    if ext in ("ndjson","jsonl"):
        df = parse_ndjson(data)
        return (df, msg) if df is not None else (None, "NDJSON parse failed.")

    try: return pd.read_csv(io.BytesIO(data), low_memory=False, encoding_errors="replace"), f"Unknown .{ext}, read as CSV."
    except Exception as e: return None, f"Could not read: {e}"


# ══════════════════════════════════════════════════════════════════════════════
#  EXPORT  — cached by (df_json, format)
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner=False)
def export_df(df_json: str, fmt: str) -> bytes | None:
    """Convert cleaned df to requested format. Cached — 0ms on repeated calls."""
    df  = pd.read_json(io.StringIO(df_json), orient="split")
    buf = io.BytesIO()
    safe = df.copy()
    if fmt in ("avro","xml","json","ndjson"):
        for col in safe.columns:
            nn = safe[col].dropna()
            if len(nn)>0 and isinstance(nn.iloc[0],dt.date) and not isinstance(nn.iloc[0],dt.datetime):
                safe[col]=safe[col].astype(str)
    if fmt in ("avro","xml"):
        for col in safe.select_dtypes(include=["datetime64[ns]","datetime64"]).columns:
            safe[col]=safe[col].astype(str)
    try:
        if fmt=="csv":     safe.to_csv(buf,index=False)
        elif fmt=="tsv":   safe.to_csv(buf,index=False,sep="\t")
        elif fmt=="xlsx":  safe.to_excel(buf,index=False,engine="openpyxl")
        elif fmt=="json":  safe.to_json(buf,orient="records",indent=2,date_format="iso")
        elif fmt=="ndjson":
            buf.write(("\n".join(json.dumps(r,default=str) for r in safe.to_dict(orient="records"))).encode())
        elif fmt=="parquet": safe.to_parquet(buf,index=False,engine="pyarrow")
        elif fmt=="avro":
            import fastavro
            tm={"int64":"long","int32":"int","float64":"double","float32":"float",
                "bool":"boolean","object":"string","datetime64[ns]":"string"}
            fields=[{"name":c,"type":["null",tm.get(str(safe[c].dtype),"string")],"default":None} for c in safe.columns]
            fastavro.writer(buf,{"type":"record","name":"CleanedData","fields":fields},
                            safe.where(safe.notna(),other=None).to_dict(orient="records"))
        elif fmt=="orc":
            import pyarrow as pa, pyarrow.orc as orc
            orc.write_table(pa.Table.from_pandas(safe,preserve_index=False),buf)
        elif fmt=="xml": safe.to_xml(buf,index=False,root_name="data",row_name="record")
        else: safe.to_csv(buf,index=False)
        return buf.getvalue()
    except Exception as e:
        st.error(f"Export error ({fmt}): {e}"); return None


# ══════════════════════════════════════════════════════════════════════════════
#  UI
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
#  UI
# ══════════════════════════════════════════════════════════════════════════════

FILE_TYPES = {
    "CSV (.csv)":              "csv",
    "TSV (.tsv)":              "tsv",
    "Excel (.xlsx / .xls)":    "xlsx",
    "JSON (.json)":            "json",
    "NDJSON / JSONL (.jsonl)": "ndjson",
    "Parquet (.parquet)":      "parquet",
    "Avro (.avro)":            "avro",
    "ORC (.orc)":              "orc",
    "XML (.xml)":              "xml",
}

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style='padding:.25rem 0 1.25rem 0;'>
      <div style='font-size:1.25rem;font-weight:700;color:#0f172a;letter-spacing:-.02em;'>
        🧹 DataFlow
      </div>
      <div style='font-size:.74rem;color:#6b7280;margin-top:.15rem;'>Parse · Clean · Export · Deliver</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='font-size:.68rem;font-weight:700;color:#9ca3af;letter-spacing:.1em;text-transform:uppercase;margin-bottom:.4rem;'>File Type</div>", unsafe_allow_html=True)
    selected_label = st.selectbox("", list(FILE_TYPES.keys()), label_visibility="collapsed")
    file_type      = FILE_TYPES[selected_label]
    st.markdown("<div style='margin-top:.75rem;font-size:.68rem;font-weight:700;color:#9ca3af;letter-spacing:.1em;text-transform:uppercase;margin-bottom:.4rem;'>Upload File</div>", unsafe_allow_html=True)
    uploaded_file = st.file_uploader("", type=None, label_visibility="collapsed",
        help="File type auto-detected from filename. Dropdown above is a hint only.")

    # ── Progress checklist states ─────────────────────────────────────────────
    _file_loaded = uploaded_file is not None
    _sf_raw_done = st.session_state.get("sf_done", False)
    _clean_done  = any(k.startswith("clean_") for k in st.session_state)
    _export_done = any(k.startswith("export_") for k in st.session_state)
    _dest_done   = st.session_state.get("sf2_done", False) or st.session_state.get("cloud_done", False)

    def _state(done, active):
        if done:   return "done"
        if active: return "active"
        return "pending"

    STEPS = [
        ("📂", "Upload File",         "Select type & upload",                 _file_loaded,  not _file_loaded),
        ("📄", "Raw Preview",         "Inspect schema & sample rows",          _file_loaded,  _file_loaded),
        ("❄️", "Load Raw → SF",       "Optional: push raw data to Snowflake",  _sf_raw_done,  _file_loaded and not _sf_raw_done),
        ("🔍", "Data Parsing",        "Flatten & structure the data",          _file_loaded,  _file_loaded),
        ("🧹", "Data Cleaning",       "Trim, type-infer, de-dupe, outliers",   _clean_done,   _file_loaded and not _clean_done),
        ("📦", "Export",              "Choose format & download",              _export_done,  _clean_done and not _export_done),
        ("🚀", "Send to Destination", "Cloud storage or Snowflake",            _dest_done,    _clean_done),
    ]

    st.markdown("<div style='font-size:.68rem;font-weight:700;color:#9ca3af;letter-spacing:.1em;text-transform:uppercase;margin:.9rem 0 .5rem 0;'>Pipeline Progress</div>", unsafe_allow_html=True)

    html_steps = ""
    for icon, label, sub, done, active in STEPS:
        s    = _state(done, active)
        icon_map = {"done": "✅", "active": "🔵", "pending": "⬜"}
        css_map  = {"done": "step-done", "active": "step-active", "pending": "step-pending"}
        html_steps += f"""<div class="step-item {css_map[s]}">
          <span class="step-icon">{icon_map[s]}</span>
          <div class="step-label">{icon} {label}
            <div class="step-sub">{sub}</div>
          </div>
        </div>"""
    st.markdown(html_steps, unsafe_allow_html=True)

    n_done = sum(1 for _, _, _, done, _ in STEPS if done)
    pct    = int(n_done / len(STEPS) * 100)
    st.markdown(f"""
    <div style='margin-top:.9rem;'>
      <div style='display:flex;justify-content:space-between;
                  font-size:.7rem;color:#6b7280;margin-bottom:.3rem;font-weight:600;'>
        <span>Overall</span><span>{pct}%</span>
      </div>
      <div style='background:#f1f5f9;border-radius:99px;height:5px;overflow:hidden;'>
        <div style='width:{pct}%;background:linear-gradient(90deg,#2563eb,#60a5fa);
                    height:100%;border-radius:99px;transition:width .4s ease;'></div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<hr style='margin:1rem 0 .75rem 0;border-color:#e5e7eb;'>", unsafe_allow_html=True)
    st.markdown("<div style='font-size:.7rem;color:#9ca3af;line-height:1.7;'>CSV · TSV · Excel · JSON<br>NDJSON · Parquet · Avro · ORC · XML</div>", unsafe_allow_html=True)

# ── Header banner ─────────────────────────────────────────────────────────────
st.markdown("""
<div class="header-banner">
  <div class="header-icon">🧹</div>
  <div>
    <div class="header-title">DataFlow — Data Cleaning Pipeline</div>
    <div class="header-sub">
      Upload any file &nbsp;·&nbsp; Auto-parse &nbsp;·&nbsp; Clean &amp; validate
      &nbsp;·&nbsp; Export to any format &nbsp;·&nbsp; Push to Cloud or Snowflake
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Landing page ──────────────────────────────────────────────────────────────
if uploaded_file is None:
    st.markdown("""
    <div style='text-align:center;padding:2rem 0 1.5rem 0;'>
      <div style='font-size:2.8rem;margin-bottom:.75rem;'>📂</div>
      <div style='font-size:1.15rem;font-weight:600;color:#374151;'>
        Select a file type in the sidebar, then upload your file to begin
      </div>
      <div style='font-size:.875rem;color:#6b7280;margin-top:.5rem;'>
        All processing happens locally · Supports 150MB+ files · No data leaves your machine
      </div>
    </div>
    """, unsafe_allow_html=True)

    FEATURES = [
        ("⚡", "Lightning Fast",    "SHA-256 caching — zero recomputation on widget re-renders. 150MB files clean in seconds."),
        ("🔍", "Smart Parsing",     "Recursive lateral flatten for JSON. Auto-detects CSV delimiters. Mirrors Snowflake LATERAL FLATTEN."),
        ("🧹", "Deep Cleaning",     "Trim · Nullish harmonisation · Type inference · Boolean detection · Duplicate removal · Outlier quarantine."),
        ("📅", "Date Detection",    "Regex-sampled on 200 rows — converts text dates & datetimes to proper dtypes automatically."),
        ("❄️", "Snowflake Ready",   "Auto-generates DDL with inferred types. Load raw or cleaned data. Auto / Append / Overwrite / Recreate."),
        ("☁️", "Cloud Delivery",    "Push directly to AWS S3, Azure Blob, or Google Cloud Storage in any of 9 supported formats."),
    ]
    r1c1, r1c2, r1c3 = st.columns(3)
    r2c1, r2c2, r2c3 = st.columns(3)
    for (icon, title, desc), col in zip(FEATURES, [r1c1,r1c2,r1c3,r2c1,r2c2,r2c3]):
        with col:
            st.markdown(f"""
            <div style='background:#fff;border:1px solid #e5e7eb;border-radius:12px;
                        padding:1.25rem 1.4rem;margin-bottom:.75rem;
                        box-shadow:0 1px 4px rgba(0,0,0,.05);height:100%;'>
              <div style='font-size:1.5rem;margin-bottom:.5rem;'>{icon}</div>
              <div style='font-size:.9rem;font-weight:600;color:#0f172a;margin-bottom:.3rem;'>{title}</div>
              <div style='font-size:.8rem;color:#6b7280;line-height:1.55;'>{desc}</div>
            </div>""", unsafe_allow_html=True)
    st.stop()

# ── Load file ─────────────────────────────────────────────────────────────────
raw_data = uploaded_file.read()
fhash    = file_hash(raw_data)
fname    = uploaded_file.name.lower()
ext      = fname.rsplit(".",1)[-1] if "." in fname else file_type

with st.spinner("Loading file…"):
    original_df, load_msg = load_file_cached(raw_data, ext, fname)

if original_df is None:
    st.error(f"Failed to load: {load_msg}"); st.stop()
if load_msg:
    st.info(load_msg)

r, c = original_df.shape

# ══════════════════════════════════════════════════════════════════════════════
#  RAW PREVIEW
# ══════════════════════════════════════════════════════════════════════════════
_section_pill("📄", "Step 1 — Raw Preview")
st.markdown("## 📄 Raw Preview")

m1, m2, m3, m4 = st.columns(4)
m1.metric("Rows",      f"{r:,}")
m2.metric("Columns",   f"{c:,}")
m3.metric("File Type", ext.upper())
m4.metric("File Size", f"{len(raw_data)/1_048_576:.1f} MB")

with st.expander("🔎 Show raw data (first 500 rows)", expanded=False):
    st.dataframe(original_df.head(500), use_container_width=True)

with st.expander("🗂️ Raw column schema", expanded=False):
    srows = []
    for col in original_df.columns:
        nn = original_df[col].dropna()
        srows.append({"Column": col, "Dtype": str(original_df[col].dtype),
            "Nulls": int(original_df[col].isnull().sum()),
            "Null %": f"{original_df[col].isnull().mean()*100:.1f}%",
            "Sample": str(nn.iloc[0])[:80] if len(nn) > 0 else "—"})
    st.dataframe(pd.DataFrame(srows), use_container_width=True)

st.markdown("---")



# ══════════════════════════════════════════════════════════════════════════════
#  ❄️ SNOWFLAKE — RAW LOAD
# ══════════════════════════════════════════════════════════════════════════════
_section_pill("❄️", "Step 2 — Load Raw Data to Snowflake (Optional)")
st.markdown("## ❄️ Load to Snowflake")
st.markdown("<p style='color:#8892a4;font-size:.9rem;'>Load the <b>raw</b> file into Snowflake before parsing/cleaning. Table auto-created with inferred types if it doesn't exist.</p>", unsafe_allow_html=True)

with st.expander("❄️ Configure & Load", expanded=False):
    st.markdown("### 🔐 Credentials")
    st.markdown("<p style='color:#fbbf24;font-size:.82rem;'>⚠️ Used for this session only. Never stored.</p>", unsafe_allow_html=True)
    cr1,cr2=st.columns(2)
    with cr1:
        sf_account  = st.text_input("Account Identifier", placeholder="myorg-myaccount")
        sf_user     = st.text_input("Username")
        sf_password = st.text_input("Password", type="password")
        sf_role     = st.text_input("Role (optional)", placeholder="SYSADMIN")
    with cr2:
        sf_warehouse= st.text_input("Warehouse", placeholder="COMPUTE_WH")
        sf_database = st.text_input("Database",  placeholder="MY_DATABASE")
        sf_schema_i = st.text_input("Schema",    placeholder="PUBLIC")
        sf_table    = st.text_input("Target Table",
            value=uploaded_file.name.rsplit(".",1)[0].upper().replace("-","_").replace(" ","_"))
    st.markdown("---")
    lc1,lc2=st.columns(2)
    with lc1:
        load_mode=st.radio("Load mode",["Auto (check if table exists)","Always overwrite","Always append","Always recreate"],index=0)
    with lc2:
        st.markdown("""<div style='font-size:.82rem;color:#8892a4;line-height:2;'>
<span style='color:#00e5a0'>Auto</span> — Exists? Append. Doesn't? Create+Insert.<br>
<span style='color:#7dd3fc'>Overwrite</span> — TRUNCATE + INSERT.<br>
<span style='color:#fbbf24'>Append</span> — INSERT into existing.<br>
<span style='color:#ef4444'>Recreate</span> — CREATE OR REPLACE + INSERT.</div>""", unsafe_allow_html=True)
    st.markdown("---")
    st.markdown("### 📋 DDL Preview")
    full_table = f"{sf_database}.{sf_schema_i}.{sf_table}" if sf_database and sf_schema_i and sf_table else (sf_table or "YOUR_TABLE")
    st.code(build_create_table_sql(full_table, original_df), language="sql")
    sp=[]
    for col in original_df.columns:
        safe_col=re.sub(r"[^a-zA-Z0-9_]","_",str(col)).strip("_").upper()
        sp.append({"Original":col,"Snowflake":safe_col,"Pandas":str(original_df[col].dtype),
                   "Snowflake Type":pd_dtype_to_snowflake(str(original_df[col].dtype),original_df[col])})
    st.dataframe(pd.DataFrame(sp), use_container_width=True)

    if st.button("❄️  Connect & Load to Snowflake"):
        missing=[f for f,v in {"Account":sf_account,"User":sf_user,"Password":sf_password,
            "Warehouse":sf_warehouse,"Database":sf_database,"Schema":sf_schema_i,"Table":sf_table}.items() if not v]
        if missing: st.error(f"Missing: {', '.join(missing)}")
        else:
            with st.spinner("Connecting…"):
                conn,err=get_snowflake_connection({"account":sf_account,"user":sf_user,"password":sf_password,
                    "warehouse":sf_warehouse,"database":sf_database,"schema":sf_schema_i,"role":sf_role})
            if err: st.error(f"Connection failed: {err}")
            else:
                st.success("✅ Connected.")
                exists=table_exists_in_snowflake(conn,sf_database,sf_schema_i,sf_table)
                mode_map={"Auto (check if table exists)":"append" if exists else "create",
                    "Always overwrite":"overwrite","Always append":"append","Always recreate":"create"}
                eff=mode_map[load_mode]
                if exists:
                    ec=get_existing_table_columns(conn,sf_database,sf_schema_i,sf_table)
                    st.info(f"Table exists ({len(ec)} cols). Mode: **{eff.upper()}**")
                    with st.expander("Existing schema"): st.dataframe(pd.DataFrame(ec),use_container_width=True)
                else:
                    st.info(f"Table doesn't exist. Will CREATE with {c} columns.")
                with st.spinner(f"Loading {r:,} rows…"):
                    res=load_df_to_snowflake(conn,full_table,original_df,exists,eff)
                conn.close()
                if res["success"]:
                    st.success(f"✅ {res['rows_loaded']:,} rows → `{full_table}`.")
                    if res["ddl"]:
                        with st.expander("DDL executed"): st.code(res["ddl"],language="sql")
                    for e in res["errors"]: st.warning(e)
                    st.session_state.update({"sf_done":True,"sf_tbl":full_table,"sf_rows":res["rows_loaded"],"sf_mode":eff})
                else:
                    st.error(f"Load failed: {'; '.join(res['errors'])}")
    elif st.session_state.get("sf_done"):
        st.success(f"✅ Previously loaded {st.session_state['sf_rows']:,} rows into `{st.session_state['sf_tbl']}`.")

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
#  DATA PARSING
# ══════════════════════════════════════════════════════════════════════════════
_section_pill("🔍", "Step 3 — Data Parsing")
st.markdown("## 🔍 Data Parsing")
if ext == "json":
    st.markdown("<p style='color:#8892a4;font-size:.9rem;'>Recursive lateral flatten applied — mirrors Snowflake <code>LATERAL FLATTEN(RECURSIVE=>TRUE)</code>.</p>", unsafe_allow_html=True)
    p1,p2,p3,p4=st.columns(4)
    nested=[col for col in original_df.columns if "_" in col]
    p1.metric("Rows After Parsing",f"{r:,}")
    p2.metric("Columns",f"{c:,}")
    p3.metric("Top-level Cols",f"{c-len(nested):,}")
    p4.metric("Flattened (nested)",f"{len(nested):,}")
    with st.expander("Column origin", expanded=False):
        sc=[]
        for col in original_df.columns:
            depth=col.count("_"); nn=original_df[col].dropna()
            sc.append({"Column":col,"Origin":"top-level" if depth==0 else f"nested (depth {depth})",
                "Dtype":str(original_df[col].dtype),"Nulls":int(original_df[col].isnull().sum()),
                "Sample":str(nn.iloc[0])[:60] if len(nn)>0 else "—"})
        st.dataframe(pd.DataFrame(sc),use_container_width=True)
    st.success(f"✅ JSON parsed: {r:,} rows × {c:,} cols. Ready for cleaning.")
else:
    st.info(f"**{ext.upper()}** is tabular — no structural parsing needed. {r:,} rows × {c:,} cols loaded.")

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
#  DATA CLEANING
#  KEY OPTIMISATION: DataFrames stored directly in session_state by fhash.
#  No to_json/read_json round-trips — eliminates 10-30s overhead on 150MB files.
# ══════════════════════════════════════════════════════════════════════════════
_section_pill("🧹", "Step 4 — Data Cleaning")
st.markdown("## 🧹 Data Cleaning")
st.markdown("<p style='color:#8892a4;font-size:.9rem;'>Vectorised pandas/numpy ops. Results stored in session — widget re-renders cost 0ms.</p>", unsafe_allow_html=True)

_CLEAN_KEY = f"clean_{fhash}"   # unique per uploaded file

if st.button("▶  Run Data Cleaning", key="run_clean"):
    with st.spinner("⚡ Cleaning…"):
        result = run_cleaning_pipeline(original_df)
    # Store DataFrames directly — no JSON serialisation
    st.session_state[_CLEAN_KEY] = result

if _CLEAN_KEY in st.session_state:
    res        = st.session_state[_CLEAN_KEY]
    cleaned_df = res["cleaned_df"]
    removed_df = res["removed_df"]
    dc         = res["date_conv"]
    os_        = res["orig_shape"]
    steps      = res["steps"]

    s1,s2,s3,s4 = st.columns(4)
    s1.metric("Original Rows",  f"{os_[0]:,}")
    s2.metric("Cleaned Rows",   f"{len(cleaned_df):,}")
    s3.metric("Removed Rows",   f"{len(removed_df):,}")
    s4.metric("Date Cols Fixed", f"{len(dc):,}")

    with st.expander("Cleaning steps log", expanded=False):
        for i, s in enumerate(steps, 1): st.markdown(f"**{i}.** {s}")

    st.markdown("### 🔬 Data Type Report")
    dm = dict(dc)
    dr = []
    for col in cleaned_df.columns:
        ob = str(original_df[col].dtype) if col in original_df.columns else "—"
        nb = str(cleaned_df[col].dtype)
        dr.append({"Column":col, "Before":ob, "After":nb,
                   "Changed":"✅" if ob!=nb else "—",
                   "Date/Time":"📅 "+dm[col] if col in dm else "—"})
    st.dataframe(pd.DataFrame(dr), use_container_width=True)

    st.markdown("### ✅ Cleaned Data")
    st.dataframe(cleaned_df.head(500), use_container_width=True)

    if not removed_df.empty:
        st.markdown("### 🗑️ Removed Rows")

        # Summary badges — extract the short category from the first sentence of reason
        if "reason" in removed_df.columns:
            # Group by first segment before " —" or ":" for a short category label
            def _short_cat(txt):
                if " —" in txt:    return txt.split(" —")[0].strip()
                if ":" in txt:     return txt.split(":")[0].replace("Row removed","").strip(" —:")
                return txt[:60]

            removed_df["_category"] = removed_df["reason"].apply(_short_cat)
            for cat, cnt in removed_df["_category"].value_counts().items():
                st.markdown(
                    f'<span class="badge badge-red">{cat}</span>'
                    f' <span style="color:#6b7280;font-size:.82rem;">&nbsp;{int(cnt)} row(s)</span>',
                    unsafe_allow_html=True)
            removed_df = removed_df.drop(columns=["_category"])

        st.markdown(
            "<p style='font-size:.82rem;color:#6b7280;margin-top:.5rem;'>"
            "The <strong>reason</strong> column explains exactly why each row was removed. "
            "This file is available for download in the Export section below."
            "</p>", unsafe_allow_html=True)

        with st.expander("🔎 Show removed rows with reason", expanded=False):
            # Show reason column first for quick scanning
            if "reason" in removed_df.columns:
                preview_cols = ["reason"] + [c for c in removed_df.columns if c != "reason"]
                st.dataframe(removed_df[preview_cols].head(500), use_container_width=True)
            else:
                st.dataframe(removed_df.head(500), use_container_width=True)

    # ── Export ───────────────────────────────────────────────────────────────
    _section_pill("📦", "Step 5 — Export")
    st.markdown("## 📦 Export")

    FMT = {
        "CSV  — universal":              ("csv",    "text/csv",                   ".csv"),
        "TSV  — tab-separated":          ("tsv",    "text/tab-separated-values",  ".tsv"),
        "Excel (.xlsx)":                 ("xlsx",   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"),
        "JSON  — records array":         ("json",   "application/json",           ".json"),
        "NDJSON — line-delimited":       ("ndjson", "application/x-ndjson",       ".ndjson"),
        "Parquet — Snowflake/Spark/dbt": ("parquet","application/octet-stream",   ".parquet"),
        "Avro — Kafka/Confluent":        ("avro",   "application/octet-stream",   ".avro"),
        "ORC  — Hive/Spark":             ("orc",    "application/octet-stream",   ".orc"),
        "XML  — legacy/SAP":             ("xml",    "application/xml",            ".xml"),
    }
    FMT_NOTES = {
        "csv":"✅ Spreadsheets, max compatibility.", "tsv":"✅ Shell pipelines.",
        "xlsx":"✅ Excel / business reports.",       "json":"✅ REST APIs, MongoDB.",
        "ndjson":"✅ Kafka, streaming.",             "parquet":"✅ Snowflake, Spark, dbt.",
        "avro":"✅ Kafka, schema evolution.",        "orc":"✅ Hive, Presto.",
        "xml":"✅ Legacy / SOAP / SAP.",
    }

    sel = st.radio("**Output format**", list(FMT.keys()), index=0)
    fk, fmime, fext = FMT[sel]
    st.markdown(f'<p style="color:#00e5a0;font-size:.85rem;">{FMT_NOTES[fk]}</p>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    def _convert_df(df: pd.DataFrame, fmt: str) -> bytes | None:
        """Vectorised export — no Python row loops. Cached result stored in session_state."""
        cache_key = f"export_{fhash}_{fmt}_{id(df)}"
        if cache_key in st.session_state:
            return st.session_state[cache_key]

        buf  = io.BytesIO()
        safe = df.copy()

        # Date objects → string for formats that need it
        if fmt in ("avro","xml","json","ndjson"):
            for col in safe.columns:
                nn = safe[col].dropna()
                if len(nn)>0 and isinstance(nn.iloc[0], dt.date) and not isinstance(nn.iloc[0], dt.datetime):
                    safe[col] = safe[col].astype(str)
        if fmt in ("avro","xml"):
            for col in safe.select_dtypes(include=["datetime64[ns]","datetime64"]).columns:
                safe[col] = safe[col].astype(str)
        try:
            if fmt=="csv":      safe.to_csv(buf, index=False)
            elif fmt=="tsv":    safe.to_csv(buf, index=False, sep="\t")
            elif fmt=="xlsx":   safe.to_excel(buf, index=False, engine="openpyxl")
            elif fmt=="json":   safe.to_json(buf, orient="records", indent=2, date_format="iso")
            elif fmt=="ndjson": buf.write(("\n".join(json.dumps(r,default=str) for r in safe.to_dict(orient="records"))).encode())
            elif fmt=="parquet": safe.to_parquet(buf, index=False, engine="pyarrow")
            elif fmt=="avro":
                import fastavro
                tm={"int64":"long","int32":"int","float64":"double","float32":"float",
                    "bool":"boolean","object":"string","datetime64[ns]":"string"}
                fields=[{"name":c,"type":["null",tm.get(str(safe[c].dtype),"string")],"default":None} for c in safe.columns]
                fastavro.writer(buf,{"type":"record","name":"CleanedData","fields":fields},
                                safe.where(safe.notna(),other=None).to_dict(orient="records"))
            elif fmt=="orc":
                import pyarrow as pa, pyarrow.orc as orc
                orc.write_table(pa.Table.from_pandas(safe, preserve_index=False), buf)
            elif fmt=="xml": safe.to_xml(buf, index=False, root_name="data", row_name="record")
            else: safe.to_csv(buf, index=False)
        except Exception as e:
            st.error(f"Export error ({fmt}): {e}"); return None

        result = buf.getvalue()
        st.session_state[cache_key] = result   # cache in session
        return result

    dl1, dl2 = st.columns(2)
    with dl1:
        if st.button(f"⚙️ Generate Cleaned File ({fext})", key="gen_c"):
            with st.spinner("Converting…"):
                cb = _convert_df(cleaned_df, fk)
            if cb:
                st.session_state[f"export_{fhash}_{fk}"] = True
                st.download_button(f"⬇️ Download Cleaned ({fext})", data=cb,
                    file_name=f"cleaned_data{fext}", mime=fmime, use_container_width=True)
    with dl2:
        if not removed_df.empty:
            if st.button(f"⚙️ Generate Removed Rows ({fext})", key="gen_r"):
                with st.spinner("Converting…"):
                    rb = _convert_df(removed_df, fk)
                if rb:
                    st.download_button(f"⬇️ Download Removed ({fext})", data=rb,
                        file_name=f"removed_rows{fext}", mime=fmime, use_container_width=True)

    st.success(
        f"Cleaned: **{len(cleaned_df):,} rows** | "
        + (f"Removed: **{len(removed_df):,} rows**" if not removed_df.empty else "No rows quarantined.")
    )

    st.markdown("---")
    _section_pill("🚀", "Step 6 — Send to Destination")
    st.markdown("## 🚀 Send Cleaned Data To...")
    st.markdown(
        "<p style='color:#8892a4;font-size:.9rem;'>"
        "Push the cleaned dataset directly to a cloud storage bucket "
        "or load it into a Snowflake table — without downloading it first."
        "</p>", unsafe_allow_html=True)

    dest_tab1, dest_tab2 = st.tabs(["☁️  Cloud Storage  (AWS / Azure / GCP)", "❄️  Snowflake"])

    # ─────────────────────────────────────────────────────────────────────────
    #  TAB 1 — CLOUD STORAGE
    # ─────────────────────────────────────────────────────────────────────────
    with dest_tab1:
        st.markdown("### ☁️ Upload to Cloud Storage")

        cloud_provider = st.selectbox(
            "**Select Cloud Provider**",
            ["— choose —", "AWS S3", "Azure Blob Storage", "Google Cloud Storage"],
            key="cloud_provider"
        )

        # ── Output format for cloud upload ───────────────────────────────────
        if cloud_provider != "— choose —":
            cloud_fmt_label = st.selectbox(
                "**File format to upload**",
                ["CSV (.csv)", "Parquet (.parquet)", "JSON (.json)",
                 "NDJSON (.ndjson)", "TSV (.tsv)", "ORC (.orc)"],
                key="cloud_fmt"
            )
            CLOUD_FMT_MAP = {
                "CSV (.csv)":        "csv",
                "Parquet (.parquet)":"parquet",
                "JSON (.json)":      "json",
                "NDJSON (.ndjson)":  "ndjson",
                "TSV (.tsv)":        "tsv",
                "ORC (.orc)":        "orc",
            }
            cloud_fmt = CLOUD_FMT_MAP[cloud_fmt_label]
            cloud_ext = f".{cloud_fmt}"

        # ── AWS S3 ───────────────────────────────────────────────────────────
        if cloud_provider == "AWS S3":
            st.markdown("#### 🔑 AWS Credentials")
            st.markdown(
                "<p style='color:#fbbf24;font-size:.82rem;'>"
                "⚠️ Credentials used for this session only. Never stored or logged. "
                "Recommended: use an IAM role with least-privilege S3 write access."
                "</p>", unsafe_allow_html=True)

            a1, a2 = st.columns(2)
            with a1:
                aws_key    = st.text_input("AWS Access Key ID",     type="password", key="aws_key")
                aws_secret = st.text_input("AWS Secret Access Key", type="password", key="aws_secret")
                aws_token  = st.text_input("Session Token (optional, for temp credentials)",
                                           type="password", key="aws_token")
            with a2:
                aws_region = st.text_input("AWS Region",   placeholder="us-east-1",  key="aws_region")
                aws_bucket = st.text_input("S3 Bucket Name", placeholder="my-data-bucket", key="aws_bucket")
                aws_prefix = st.text_input("S3 Key / Path",
                    placeholder="data/cleaned/output.csv",
                    help="Full object key including filename and extension",
                    key="aws_prefix")

            if not aws_prefix.endswith(cloud_ext):
                aws_prefix_display = aws_prefix.rstrip("/") + f"/cleaned_data{cloud_ext}" if aws_prefix else f"cleaned_data{cloud_ext}"
            else:
                aws_prefix_display = aws_prefix

            st.markdown(
                f"<p style='color:#8892a4;font-size:.82rem;'>"
                f"Target: <code>s3://{aws_bucket or 'bucket'}/{aws_prefix_display}</code>"
                f"</p>", unsafe_allow_html=True)

            if st.button("⬆️  Upload to S3", key="upload_s3"):
                missing = [f for f, v in {"Access Key": aws_key, "Secret Key": aws_secret,
                    "Region": aws_region, "Bucket": aws_bucket, "Path": aws_prefix}.items() if not v]
                if missing:
                    st.error(f"Missing: {', '.join(missing)}")
                else:
                    with st.spinner("Converting and uploading to S3…"):
                        try:
                            import boto3
                            file_bytes = _convert_df(cleaned_df, cloud_fmt)
                            if file_bytes:
                                s3 = boto3.client(
                                    "s3",
                                    aws_access_key_id     = aws_key,
                                    aws_secret_access_key = aws_secret,
                                    aws_session_token     = aws_token or None,
                                    region_name           = aws_region,
                                )
                                key = aws_prefix if aws_prefix.endswith(cloud_ext) else aws_prefix_display
                                s3.put_object(Bucket=aws_bucket, Key=key, Body=file_bytes)
                                st.success(f"✅ Uploaded to `s3://{aws_bucket}/{key}` ({len(file_bytes)/1_048_576:.2f} MB)")
                                st.session_state["cloud_done"] = True
                        except ImportError:
                            st.error("Install boto3: `pip install boto3`")
                        except Exception as e:
                            st.error(f"S3 upload failed: {e}")

        # ── Azure Blob Storage ───────────────────────────────────────────────
        elif cloud_provider == "Azure Blob Storage":
            st.markdown("#### 🔑 Azure Credentials")
            st.markdown(
                "<p style='color:#fbbf24;font-size:.82rem;'>"
                "⚠️ Credentials used for this session only. "
                "Use a SAS token or connection string with least-privilege write access."
                "</p>", unsafe_allow_html=True)

            az_auth = st.radio("Authentication method",
                ["Connection String", "Account Name + Account Key", "SAS Token"],
                horizontal=True, key="az_auth")

            az1, az2 = st.columns(2)
            with az1:
                if az_auth == "Connection String":
                    az_conn_str = st.text_input("Connection String", type="password", key="az_conn")
                elif az_auth == "Account Name + Account Key":
                    az_account  = st.text_input("Storage Account Name", key="az_account")
                    az_acct_key = st.text_input("Account Key", type="password", key="az_key")
                else:
                    az_account  = st.text_input("Storage Account Name", key="az_account_sas")
                    az_sas      = st.text_input("SAS Token (starts with ?sv=...)", type="password", key="az_sas")
            with az2:
                az_container = st.text_input("Container Name",  placeholder="my-container", key="az_container")
                az_blob_path = st.text_input("Blob Path / Filename",
                    placeholder="data/cleaned/output.csv",
                    help="Full blob name including any folder prefix",
                    key="az_blob_path")

            if az_blob_path and not az_blob_path.endswith(cloud_ext):
                az_blob_display = az_blob_path.rstrip("/") + f"/cleaned_data{cloud_ext}"
            else:
                az_blob_display = az_blob_path or f"cleaned_data{cloud_ext}"

            st.markdown(
                f"<p style='color:#8892a4;font-size:.82rem;'>"
                f"Target: <code>https://[account].blob.core.windows.net/{az_container or 'container'}/{az_blob_display}</code>"
                f"</p>", unsafe_allow_html=True)

            if st.button("⬆️  Upload to Azure Blob", key="upload_az"):
                with st.spinner("Converting and uploading to Azure…"):
                    try:
                        from azure.storage.blob import BlobServiceClient
                        file_bytes = _convert_df(cleaned_df, cloud_fmt)
                        if file_bytes:
                            if az_auth == "Connection String":
                                client = BlobServiceClient.from_connection_string(az_conn_str)
                            elif az_auth == "Account Name + Account Key":
                                client = BlobServiceClient(
                                    account_url=f"https://{az_account}.blob.core.windows.net",
                                    credential=az_acct_key)
                            else:
                                client = BlobServiceClient(
                                    account_url=f"https://{az_account}.blob.core.windows.net",
                                    credential=az_sas)
                            blob = client.get_blob_client(container=az_container, blob=az_blob_display)
                            blob.upload_blob(file_bytes, overwrite=True)
                            st.success(f"✅ Uploaded to Azure Blob: `{az_container}/{az_blob_display}` ({len(file_bytes)/1_048_576:.2f} MB)")
                            st.session_state["cloud_done"] = True
                    except ImportError:
                        st.error("Install azure-storage-blob: `pip install azure-storage-blob`")
                    except Exception as e:
                        st.error(f"Azure upload failed: {e}")

        # ── Google Cloud Storage ─────────────────────────────────────────────
        elif cloud_provider == "Google Cloud Storage":
            st.markdown("#### 🔑 GCP Credentials")
            st.markdown(
                "<p style='color:#fbbf24;font-size:.82rem;'>"
                "⚠️ Credentials used for this session only. "
                "Use a service account with Storage Object Creator role."
                "</p>", unsafe_allow_html=True)

            gcp_auth = st.radio("Authentication method",
                ["Service Account JSON (paste)", "Service Account JSON (upload)"],
                horizontal=True, key="gcp_auth")

            g1, g2 = st.columns(2)
            with g1:
                if gcp_auth == "Service Account JSON (paste)":
                    gcp_sa_json = st.text_area("Service Account JSON",
                        placeholder='{"type": "service_account", "project_id": "...", ...}',
                        height=120, key="gcp_sa_json")
                else:
                    gcp_sa_file = st.file_uploader("Upload service account JSON", type=["json"], key="gcp_sa_file")
                    gcp_sa_json = gcp_sa_file.read().decode() if gcp_sa_file else ""
            with g2:
                gcp_bucket  = st.text_input("GCS Bucket Name",  placeholder="my-gcs-bucket", key="gcp_bucket")
                gcp_blob    = st.text_input("Object Path / Filename",
                    placeholder="data/cleaned/output.csv",
                    help="Full object name including folder prefix",
                    key="gcp_blob")

            if gcp_blob and not gcp_blob.endswith(cloud_ext):
                gcp_blob_display = gcp_blob.rstrip("/") + f"/cleaned_data{cloud_ext}"
            else:
                gcp_blob_display = gcp_blob or f"cleaned_data{cloud_ext}"

            st.markdown(
                f"<p style='color:#8892a4;font-size:.82rem;'>"
                f"Target: <code>gs://{gcp_bucket or 'bucket'}/{gcp_blob_display}</code>"
                f"</p>", unsafe_allow_html=True)

            if st.button("⬆️  Upload to GCS", key="upload_gcs"):
                missing = [f for f, v in {"Service Account JSON": gcp_sa_json,
                    "Bucket": gcp_bucket, "Object Path": gcp_blob}.items() if not v]
                if missing:
                    st.error(f"Missing: {', '.join(missing)}")
                else:
                    with st.spinner("Converting and uploading to GCS…"):
                        try:
                            from google.cloud import storage as gcs
                            from google.oauth2 import service_account
                            sa_info  = json.loads(gcp_sa_json)
                            creds    = service_account.Credentials.from_service_account_info(sa_info)
                            gcs_client = gcs.Client(credentials=creds, project=sa_info.get("project_id"))
                            file_bytes = _convert_df(cleaned_df, cloud_fmt)
                            if file_bytes:
                                bucket = gcs_client.bucket(gcp_bucket)
                                blob   = bucket.blob(gcp_blob_display)
                                blob.upload_from_string(file_bytes)
                                st.success(f"✅ Uploaded to `gs://{gcp_bucket}/{gcp_blob_display}` ({len(file_bytes)/1_048_576:.2f} MB)")
                                st.session_state["cloud_done"] = True
                        except ImportError:
                            st.error("Install GCS SDK: `pip install google-cloud-storage`")
                        except Exception as e:
                            st.error(f"GCS upload failed: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    #  TAB 2 — SNOWFLAKE
    # ─────────────────────────────────────────────────────────────────────────
    with dest_tab2:
        st.markdown("### ❄️ Load Cleaned Data into Snowflake")
        st.markdown(
            "<p style='color:#8892a4;font-size:.9rem;'>"
            "Loads the <strong>cleaned</strong> DataFrame directly into Snowflake. "
            "If the table exists, choose append / overwrite / recreate. "
            "If it doesn't exist, it's auto-created with inferred column types."
            "</p>", unsafe_allow_html=True)

        st.markdown("#### 🔐 Credentials")
        st.markdown(
            "<p style='color:#fbbf24;font-size:.82rem;'>"
            "⚠️ Used for this session only. Never stored or logged."
            "</p>", unsafe_allow_html=True)

        sf1, sf2 = st.columns(2)
        with sf1:
            sf2_account   = st.text_input("Account Identifier",
                placeholder="myorg-myaccount  or  myaccount.region.cloud",
                key="sf2_account")
            sf2_user      = st.text_input("Username",  key="sf2_user")
            sf2_password  = st.text_input("Password",  type="password", key="sf2_password")
            sf2_role      = st.text_input("Role (optional)", placeholder="SYSADMIN", key="sf2_role")
        with sf2:
            sf2_warehouse = st.text_input("Warehouse",  placeholder="COMPUTE_WH",   key="sf2_warehouse")
            sf2_database  = st.text_input("Database",   placeholder="MY_DATABASE",  key="sf2_database")
            sf2_schema    = st.text_input("Schema",     placeholder="PUBLIC",        key="sf2_schema")
            sf2_table     = st.text_input("Target Table Name",
                value=uploaded_file.name.rsplit(".",1)[0].upper().replace("-","_").replace(" ","_") + "_CLEANED",
                key="sf2_table",
                help="Table name in Snowflake. Special chars → underscores.")

        st.markdown("---")
        st.markdown("#### ⚙️ Load Mode")
        sf_lm1, sf_lm2 = st.columns(2)
        with sf_lm1:
            sf2_load_mode = st.radio(
                "How to handle existing table",
                ["Auto (append if exists, create if not)",
                 "Overwrite (TRUNCATE + INSERT)",
                 "Append (INSERT only)",
                 "Recreate (CREATE OR REPLACE + INSERT)"],
                key="sf2_load_mode")
        with sf_lm2:
            st.markdown("""<div style='font-size:.82rem;color:#8892a4;line-height:2.2;margin-top:.5rem;'>
<span style='color:#00e5a0;font-weight:700'>Auto</span> — Smartest choice. Detects table state automatically.<br>
<span style='color:#7dd3fc;font-weight:700'>Overwrite</span> — Clears existing data, loads fresh.<br>
<span style='color:#fbbf24;font-weight:700'>Append</span> — Adds rows without touching existing data.<br>
<span style='color:#ef4444;font-weight:700'>Recreate</span> — Drops old schema entirely. Use with care.
</div>""", unsafe_allow_html=True)

        st.markdown("---")
        st.markdown("#### 📋 DDL Preview (will be executed if table doesn't exist)")
        sf2_full_table = (f"{sf2_database}.{sf2_schema}.{sf2_table}"
                          if sf2_database and sf2_schema and sf2_table
                          else (sf2_table or "YOUR_DB.YOUR_SCHEMA.YOUR_TABLE"))
        st.code(build_create_table_sql(sf2_full_table, cleaned_df), language="sql")

        # Schema mapping table
        sfsp = []
        for col in cleaned_df.columns:
            sc = re.sub(r"[^a-zA-Z0-9_]","_",str(col)).strip("_").upper()
            sfsp.append({"Original Column": col, "Snowflake Column": sc,
                         "Pandas Dtype": str(cleaned_df[col].dtype),
                         "Snowflake Type": pd_dtype_to_snowflake(str(cleaned_df[col].dtype), cleaned_df[col])})
        with st.expander("Column type mapping", expanded=False):
            st.dataframe(pd.DataFrame(sfsp), use_container_width=True)

        st.markdown("<br>", unsafe_allow_html=True)

        if st.button("❄️  Connect & Load Cleaned Data to Snowflake", key="sf2_load_btn"):
            missing_sf = [f for f, v in {
                "Account":   sf2_account,  "Username":  sf2_user,
                "Password":  sf2_password, "Warehouse": sf2_warehouse,
                "Database":  sf2_database, "Schema":    sf2_schema,
                "Table":     sf2_table,
            }.items() if not v]
            if missing_sf:
                st.error(f"Missing required fields: {', '.join(missing_sf)}")
            else:
                with st.spinner("Connecting to Snowflake…"):
                    conn2, err2 = get_snowflake_connection({
                        "account":   sf2_account,  "user":     sf2_user,
                        "password":  sf2_password, "warehouse":sf2_warehouse,
                        "database":  sf2_database, "schema":   sf2_schema,
                        "role":      sf2_role,
                    })
                if err2:
                    st.error(f"Connection failed: {err2}")
                else:
                    st.success("✅ Connected to Snowflake.")
                    exists2 = table_exists_in_snowflake(conn2, sf2_database, sf2_schema, sf2_table)

                    mode_map2 = {
                        "Auto (append if exists, create if not)": "append" if exists2 else "create",
                        "Overwrite (TRUNCATE + INSERT)":          "overwrite",
                        "Append (INSERT only)":                   "append",
                        "Recreate (CREATE OR REPLACE + INSERT)":  "create",
                    }
                    eff2 = mode_map2[sf2_load_mode]

                    if exists2:
                        ec2 = get_existing_table_columns(conn2, sf2_database, sf2_schema, sf2_table)
                        st.info(f"Table `{sf2_full_table}` exists ({len(ec2)} columns). Mode: **{eff2.upper()}**")
                        with st.expander("Existing Snowflake schema"):
                            st.dataframe(pd.DataFrame(ec2), use_container_width=True)
                    else:
                        st.info(f"Table `{sf2_full_table}` doesn't exist → will **CREATE** with {len(cleaned_df.columns)} columns.")

                    with st.spinner(f"Loading {len(cleaned_df):,} cleaned rows into Snowflake…"):
                        res2 = load_df_to_snowflake(conn2, sf2_full_table, cleaned_df, exists2, eff2)
                    conn2.close()

                    if res2["success"]:
                        st.success(
                            f"✅ **{res2['rows_loaded']:,} rows** loaded into "
                            f"`{sf2_full_table}` (mode: **{eff2.upper()}**).")
                        if res2["ddl"]:
                            with st.expander("DDL executed"):
                                st.code(res2["ddl"], language="sql")
                        for e in res2["errors"]:
                            st.warning(f"Non-fatal: {e}")
                        st.session_state.update({
                            "sf2_done": True, "sf2_tbl": sf2_full_table,
                            "sf2_rows": res2["rows_loaded"], "sf2_mode": eff2,
                        })
                    else:
                        st.error(f"Load failed: {'; '.join(res2['errors'])}")

        elif st.session_state.get("sf2_done"):
            st.success(
                f"✅ Previously loaded **{st.session_state['sf2_rows']:,} rows** "
                f"into `{st.session_state['sf2_tbl']}` "
                f"(mode: **{st.session_state['sf2_mode'].upper()}**).")
