"""
DataFlow Backend — Single File
Run: uvicorn main:app --reload
"""
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd, numpy as np, json, io, re, os
from datetime import datetime

app = FastAPI(title="DataFlow API", version="1.0.0")

app.add_middleware(CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/")
def root(): return {"status":"ok","service":"DataFlow API","version":"1.0.0"}

@app.get("/health")
def health(): return {"status":"ok"}

# ── Clean endpoint ────────────────────────────────────────────────────────────
@app.post("/clean")
async def clean_file(file: UploadFile = File(...)):
    data  = await file.read()
    ext   = file.filename.rsplit(".",1)[-1].lower() if "." in file.filename else "csv"
    df    = load(data, ext)
    if df is None:
        raise HTTPException(400, "Could not parse file")
    result = clean(df)
    return {
        "original_rows":  result["orig"][0],
        "cleaned_rows":   len(result["cleaned"]),
        "removed_rows":   len(result["removed"]),
        "columns":        len(df.columns),
        "steps":          result["steps"],
        "cleaned_preview":result["cleaned"].head(5).to_dict(orient="records"),
        "removed_preview":result["removed"].head(5).to_dict(orient="records") if not result["removed"].empty else [],
    }

# ── Contact form ──────────────────────────────────────────────────────────────
class ContactForm(BaseModel):
    name: str; company: str; email: str
    migration_type: str = ""; message: str = ""

@app.post("/contact")
def contact(form: ContactForm):
    # In production: send via Resend API
    print(f"New lead: {form.name} | {form.company} | {form.email}")
    return {"status":"ok","message":"We'll reply within 4 hours"}

# ── File loader ───────────────────────────────────────────────────────────────
def load(data:bytes, ext:str):
    try:
        if ext=="csv":
            import csv as _c
            try:
                d=_c.Sniffer().sniff(data[:4096].decode("utf-8",errors="replace"),delimiters=",;\t|")
                sep=d.delimiter
            except: sep=","
            return pd.read_csv(io.BytesIO(data),sep=sep,low_memory=False,encoding_errors="replace")
        if ext in("xls","xlsx"): return pd.read_excel(io.BytesIO(data))
        if ext=="json":
            raw=json.loads(data)
            if isinstance(raw,list): return pd.json_normalize(raw,sep="_")
            if isinstance(raw,dict):
                for v in raw.values():
                    if isinstance(v,list): return pd.json_normalize(v,sep="_")
                return pd.json_normalize([raw],sep="_")
        if ext=="parquet": return pd.read_parquet(io.BytesIO(data))
    except: pass
    return None

# ── Cleaning engine ───────────────────────────────────────────────────────────
_NULL = frozenset({"na","n/a","null","none","nan","-","","undefined","nil"})
_BOOL = {"true":True,"false":False,"yes":True,"no":False,"1":True,"0":False}
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$|^\d{2}/\d{2}/\d{4}$")
_DT   = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")

def clean(df:pd.DataFrame)->dict:
    steps=[]; removed=[]; orig=df.shape
    df.columns = df.columns.str.strip()
    for c in df.select_dtypes(include="object").columns:
        df[c]=df[c].str.strip()
    for c in df.select_dtypes(include="object").columns:
        mask=df[c].str.strip().str.lower().isin(_NULL)
        if mask.any(): df.loc[mask,c]=np.nan
    null_mask=df.isnull().all(axis=1)
    if null_mask.any():
        sub=df[null_mask].copy(); sub["reason"]="All values missing"
        removed.append(sub); df=df[~null_mask]; steps.append(f"Removed {null_mask.sum()} empty rows")
    dup_mask=df.duplicated(keep="first")
    if dup_mask.any():
        sub=df[dup_mask].copy(); sub["reason"]="Duplicate row"
        removed.append(sub); df=df[~dup_mask]; steps.append(f"Removed {dup_mask.sum()} duplicates")
    for c in df.select_dtypes(include="object").columns:
        conv=pd.to_numeric(df[c],errors="coerce")
        if df[c].notna().sum()>0 and conv.notna().sum()/df[c].notna().sum()>=0.85:
            df[c]=conv; steps.append(f"'{c}': text→numeric")
    num=df.select_dtypes(include=[np.number]).columns.tolist()
    if num:
        mat=df[num].to_numpy(dtype=float,na_value=np.nan)
        z=np.abs((mat-np.nanmean(mat,0))/(np.nanstd(mat,0)+1e-9))
        out=np.any(z>5,axis=1)
        if out.sum()>0:
            sub=df[out].copy(); sub["reason"]="Statistical outlier (z>5)"
            removed.append(sub); df=df[~out]; steps.append(f"Removed {out.sum()} outliers")
    removed_df=pd.concat(removed,ignore_index=True) if removed else pd.DataFrame()
    return {"cleaned":df.reset_index(drop=True),"removed":removed_df,"steps":steps,"orig":orig}
