"""
API Gateway for NarcoLakePtic Data Lake
Provides REST endpoints to access Raw, Staging, and Curated layer data.
Required endpoints per PDF Section 2.5:
- GET /raw - Access raw data
- GET /staging - Access staging data
- GET /curated - Access curated data
- GET /health - Service health check
- GET /stats - Bucket/database metrics
"""
import os
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
import pandas as pd
from minio import Minio
from minio.error import S3Error

# Load environment
from dotenv import load_dotenv
load_dotenv()

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_RAW = os.getenv("MINIO_BUCKET_RAW", "raw")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

DATA_STAGING_PATH = Path(os.getenv("DATA_STAGING_PATH", "data/staging"))
DATA_CURATED_PATH = Path(os.getenv("DATA_CURATED_PATH", "data/curated"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# MinIO client
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

# FastAPI app
app = FastAPI(
    title="NarcoLakePtic API Gateway",
    description="Data Lake API for drug trend analysis - Raw/Staging/Curated layers",
    version="1.0.0"
)


# Response models
class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    minio_connected: bool
    staging_accessible: bool
    curated_accessible: bool


class StatsResponse(BaseModel):
    raw_bucket: Dict[str, Any]
    staging: Dict[str, Any]
    curated: Dict[str, Any]


class LayerFileInfo(BaseModel):
    name: str
    size_bytes: int
    modified: str
    rows: Optional[int] = None
    columns: Optional[int] = None


# ========== UTILITY FUNCTIONS ==========

def get_minio_objects(prefix: str = "") -> List[Dict]:
    """List objects in MinIO raw bucket."""
    try:
        objects = []
        for obj in minio_client.list_objects(MINIO_BUCKET_RAW, prefix=prefix, recursive=True):
            objects.append({
                "name": obj.object_name,
                "size_bytes": obj.size,
                "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
                "etag": obj.etag
            })
        return objects
    except S3Error as e:
        logger.error(f"MinIO error listing objects: {e}")
        return []


def read_parquet_info(filepath: Path) -> Dict[str, Any]:
    """Read basic info from a Parquet file without loading full data."""
    try:
        df = pd.read_parquet(filepath)
        return {
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": df.columns.tolist(),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()}
        }
    except Exception as e:
        logger.error(f"Error reading {filepath}: {e}")
        return {"error": str(e)}


def read_parquet_sample(filepath: Path, limit: int = 100) -> List[Dict]:
    """Read a sample of rows from a Parquet file."""
    try:
        df = pd.read_parquet(filepath)
        if len(df) > limit:
            df = df.head(limit)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error reading sample from {filepath}: {e}")
        return []


# ========== HEALTH ENDPOINT ==========

@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """
    Health check endpoint.
    Verifies connectivity to MinIO and accessibility of staging/curated directories.
    """
    # Check MinIO
    minio_ok = False
    try:
        minio_client.list_buckets()
        minio_ok = True
    except Exception:
        pass

    # Check staging
    staging_ok = DATA_STAGING_PATH.exists() and any(DATA_STAGING_PATH.glob("*.parquet"))

    # Check curated
    curated_ok = DATA_CURATED_PATH.exists() and any(DATA_CURATED_PATH.glob("*.parquet"))

    overall = "healthy" if (minio_ok and staging_ok and curated_ok) else "degraded"

    return HealthResponse(
        status=overall,
        service="NarcoLakePtic API Gateway",
        version="1.0.0",
        minio_connected=minio_ok,
        staging_accessible=staging_ok,
        curated_accessible=curated_ok
    )


# ========== STATS ENDPOINT ==========

@app.get("/stats", response_model=StatsResponse, tags=["System"])
async def get_stats():
    """
    Statistics endpoint.
    Returns metrics on bucket sizes, file counts, row counts per layer.
    """
    # Raw bucket stats
    raw_objects = get_minio_objects()
    raw_total_size = sum(obj["size_bytes"] for obj in raw_objects)
    raw_stats = {
        "bucket": MINIO_BUCKET_RAW,
        "object_count": len(raw_objects),
        "total_size_bytes": raw_total_size,
        "total_size_mb": round(raw_total_size / (1024 * 1024), 2),
        "objects": raw_objects
    }

    # Staging stats
    staging_files = list(DATA_STAGING_PATH.glob("*.parquet"))
    staging_data = []
    staging_total_rows = 0
    staging_total_size = 0
    for f in staging_files:
        info = read_parquet_info(f)
        stat = f.stat()
        staging_total_rows += info.get("rows", 0)
        staging_total_size += stat.st_size
        staging_data.append({
            "name": f.name,
            "size_bytes": stat.st_size,
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "rows": info.get("rows", 0),
            "columns": info.get("columns", 0),
            "column_names": info.get("column_names", []),
            "modified": stat.st_mtime
        })

    staging_stats = {
        "path": str(DATA_STAGING_PATH),
        "file_count": len(staging_files),
        "total_size_bytes": staging_total_size,
        "total_size_mb": round(staging_total_size / (1024 * 1024), 2),
        "total_rows": staging_total_rows,
        "files": staging_data
    }

    # Curated stats
    curated_files = list(DATA_CURATED_PATH.glob("*.parquet"))
    curated_data = []
    curated_total_rows = 0
    curated_total_size = 0
    for f in curated_files:
        info = read_parquet_info(f)
        stat = f.stat()
        curated_total_rows += info.get("rows", 0)
        curated_total_size += stat.st_size
        curated_data.append({
            "name": f.name,
            "size_bytes": stat.st_size,
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "rows": info.get("rows", 0),
            "columns": info.get("columns", 0),
            "column_names": info.get("column_names", []),
            "modified": stat.st_mtime
        })

    curated_stats = {
        "path": str(DATA_CURATED_PATH),
        "file_count": len(curated_files),
        "total_size_bytes": curated_total_size,
        "total_size_mb": round(curated_total_size / (1024 * 1024), 2),
        "total_rows": curated_total_rows,
        "files": curated_data
    }

    return StatsResponse(
        raw_bucket=raw_stats,
        staging=staging_stats,
        curated=curated_stats
    )


# ========== RAW LAYER ENDPOINT ==========

@app.get("/raw", tags=["Raw Layer"])
async def list_raw(
    prefix: str = Query("", description="Filter by object prefix"),
    limit: int = Query(100, description="Max objects to return"),
    download: bool = Query(False, description="Download file instead of listing")
):
    """
    Access raw data in MinIO bucket.
    - Lists objects with metadata
    - Supports optional download of a specific file
    """
    objects = get_minio_objects(prefix=prefix)
    objects = objects[:limit]

    if download and prefix:
        # Download specific object
        try:
            response = minio_client.get_object(MINIO_BUCKET_RAW, prefix)
            data = response.read()
            response.close()
            response.release_conn()
            return Response(
                content=data,
                media_type="application/octet-stream",
                headers={"Content-Disposition": f"attachment; filename={Path(prefix).name}"}
            )
        except S3Error as e:
            raise HTTPException(status_code=404, detail=f"Object not found: {e}")

    return {
        "bucket": MINIO_BUCKET_RAW,
        "prefix": prefix,
        "count": len(objects),
        "objects": objects
    }


@app.get("/raw/{object_name:path}", tags=["Raw Layer"])
async def get_raw_object(object_name: str):
    """Download a specific raw object from MinIO."""
    try:
        response = minio_client.get_object(MINIO_BUCKET_RAW, object_name)
        data = response.read()
        response.close()
        response.release_conn()
        return Response(
            content=data,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={Path(object_name).name}"}
        )
    except S3Error as e:
        raise HTTPException(status_code=404, detail=f"Object not found: {e}")


# ========== STAGING LAYER ENDPOINT ==========

@app.get("/staging", tags=["Staging Layer"])
async def list_staging(
    limit: int = Query(100, description="Max rows per file sample"),
    file: Optional[str] = Query(None, description="Specific file to sample")
):
    """
    Access staging layer data.
    Lists available Parquet files with metadata and optional samples.
    """
    staging_files = list(DATA_STAGING_PATH.glob("*.parquet"))

    if file:
        # Return sample of specific file
        fpath = DATA_STAGING_PATH / file
        if not fpath.exists():
            raise HTTPException(status_code=404, detail=f"File not found: {file}")
        sample = read_parquet_sample(fpath, limit)
        info = read_parquet_info(fpath)
        return {
            "file": file,
            "info": info,
            "sample": sample,
            "sample_size": len(sample)
        }

    # List all files with info
    files_info = []
    for f in staging_files:
        info = read_parquet_info(f)
        stat = f.stat()
        files_info.append({
            "name": f.name,
            "size_bytes": stat.st_size,
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "rows": info.get("rows", 0),
            "columns": info.get("columns", 0),
            "column_names": info.get("column_names", []),
            "dtypes": info.get("dtypes", {}),
            "modified": stat.st_mtime
        })

    return {
        "path": str(DATA_STAGING_PATH),
        "file_count": len(files_info),
        "files": files_info
    }


@app.get("/staging/{filename}", tags=["Staging Layer"])
async def get_staging_file(
    filename: str,
    limit: int = Query(1000, description="Max rows to return"),
    offset: int = Query(0, description="Row offset for pagination"),
    format: str = Query("json", description="Output format: json, csv, parquet")
):
    """Get data from a specific staging Parquet file."""
    fpath = DATA_STAGING_PATH / filename
    if not fpath.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")

    try:
        df = pd.read_parquet(fpath)
        total_rows = len(df)

        if offset >= total_rows:
            return {"data": [], "total_rows": total_rows, "returned": 0, "offset": offset}

        df_slice = df.iloc[offset:offset + limit]

        if format == "csv":
            csv_data = df_slice.to_csv(index=False)
            return Response(
                content=csv_data,
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}.csv"}
            )
        elif format == "parquet":
            import io
            buf = io.BytesIO()
            df_slice.to_parquet(buf, index=False)
            buf.seek(0)
            return Response(
                content=buf.read(),
                media_type="application/octet-stream",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        else:
            return {
                "file": filename,
                "total_rows": total_rows,
                "offset": offset,
                "limit": limit,
                "returned": len(df_slice),
                "columns": df_slice.columns.tolist(),
                "data": df_slice.to_dict(orient="records")
            }
    except Exception as e:
        logger.error(f"Error reading {filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading file: {e}")


# ========== CURATED LAYER ENDPOINT ==========

@app.get("/curated", tags=["Curated Layer"])
async def list_curated(
    limit: int = Query(100, description="Max rows per file sample"),
    file: Optional[str] = Query(None, description="Specific file to sample")
):
    """
    Access curated layer data.
    Lists available Parquet files with metadata and optional samples.
    """
    curated_files = list(DATA_CURATED_PATH.glob("*.parquet"))

    if file:
        # Return sample of specific file
        fpath = DATA_CURATED_PATH / file
        if not fpath.exists():
            raise HTTPException(status_code=404, detail=f"File not found: {file}")
        sample = read_parquet_sample(fpath, limit)
        info = read_parquet_info(fpath)
        return {
            "file": file,
            "info": info,
            "sample": sample,
            "sample_size": len(sample)
        }

    # List all files with info
    files_info = []
    for f in curated_files:
        info = read_parquet_info(f)
        stat = f.stat()
        files_info.append({
            "name": f.name,
            "size_bytes": stat.st_size,
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "rows": info.get("rows", 0),
            "columns": info.get("columns", 0),
            "column_names": info.get("column_names", []),
            "dtypes": info.get("dtypes", {}),
            "modified": stat.st_mtime
        })

    return {
        "path": str(DATA_CURATED_PATH),
        "file_count": len(files_info),
        "files": files_info
    }


@app.get("/curated/{filename}", tags=["Curated Layer"])
async def get_curated_file(
    filename: str,
    limit: int = Query(1000, description="Max rows to return"),
    offset: int = Query(0, description="Row offset for pagination"),
    format: str = Query("json", description="Output format: json, csv, parquet")
):
    """Get data from a specific curated Parquet file."""
    fpath = DATA_CURATED_PATH / filename
    if not fpath.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")

    try:
        df = pd.read_parquet(fpath)
        total_rows = len(df)

        if offset >= total_rows:
            return {"data": [], "total_rows": total_rows, "returned": 0, "offset": offset}

        df_slice = df.iloc[offset:offset + limit]

        if format == "csv":
            csv_data = df_slice.to_csv(index=False)
            return Response(
                content=csv_data,
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}.csv"}
            )
        elif format == "parquet":
            import io
            buf = io.BytesIO()
            df_slice.to_parquet(buf, index=False)
            buf.seek(0)
            return Response(
                content=buf.read(),
                media_type="application/octet-stream",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
        else:
            return {
                "file": filename,
                "total_rows": total_rows,
                "offset": offset,
                "limit": limit,
                "returned": len(df_slice),
                "columns": df_slice.columns.tolist(),
                "data": df_slice.to_dict(orient="records")
            }
    except Exception as e:
        logger.error(f"Error reading {filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Error reading file: {e}")


# ========== ENTRYPOINT ==========

if __name__ == "__main__":
    import uvicorn
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)