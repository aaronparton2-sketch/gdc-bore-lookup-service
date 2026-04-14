"""
FastAPI service wrapping bore-depth-lookup logic.

Endpoints:
  GET  /healthz                  - liveness check (used by keep-warm ping)
  POST /lookup                   - depth lookup (requires X-API-Key header)
  GET  /                         - basic info page

Deploy: Render free tier (see README.md).
Cold starts (~30-60s) on free tier — keep alive via cron-job.com or similar.
"""
import os
import sys
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel

# Import the lookup logic from the sibling module
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import bore_depth_lookup as bdl  # noqa: E402

API_KEY = os.environ.get("LOOKUP_API_KEY", "")

app = FastAPI(
    title="GDC Bore Depth Lookup",
    description="Estimates likely drilling depth from QLD Groundwater DB",
    version="1.0.0",
)


class LookupRequest(BaseModel):
    address: Optional[str] = None
    suburb: Optional[str] = None
    postcode: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    radius_km: float = 5.0
    max_bores: int = 10


class LookupResponse(BaseModel):
    depth: Optional[int] = None
    depth_p75: Optional[int] = None
    depth_max: Optional[int] = None
    depth_range: Optional[str] = None
    aquifer: Optional[str] = None
    swl: Optional[int] = None
    yield_lps: Optional[float] = None
    confidence: int = 0
    confidence_label: str = "Very Low"
    resolved_tier: Optional[str] = None
    n_bores: int = 0
    errors: list[str] = []
    attribution: str = bdl.ATTRIBUTION


@app.get("/")
def root():
    return {
        "service": "GDC Bore Depth Lookup",
        "endpoints": ["/healthz", "/lookup (POST)"],
        "data_source": "QLD Government Groundwater Database (CC BY 4.0)",
    }


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/debug/bore/{rn}")
def debug_bore(rn: str, x_api_key: Optional[str] = Header(None)):
    """Fetch one bore report and return raw diagnostic info."""
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")
    import io
    import requests
    url = bdl.BORE_REPORT_URL.format(rn=rn)
    try:
        r = requests.get(url, timeout=30)
        info = {
            "url": url,
            "status_code": r.status_code,
            "content_length": len(r.content),
            "content_type": r.headers.get("content-type", "?"),
            "first_bytes_hex": r.content[:32].hex() if r.content else None,
        }
        if r.ok and len(r.content) >= 500:
            try:
                from pypdf import PdfReader
                reader = PdfReader(io.BytesIO(r.content))
                text = ""
                for p in reader.pages:
                    text += p.extract_text() or ""
                info["pages"] = len(reader.pages)
                info["text_length"] = len(text)
                info["text_full"] = text
                # Try the actual lookup parsing on this text
                import re
                STRATA = re.compile(r"^\s*\d+\s+([\d.]+)\s+([\d.]+)\s+(.+?)$", re.MULTILINE)
                strata = STRATA.findall(text)
                info["strata_rows_found"] = len(strata)
                if strata:
                    info["strata_first"] = strata[0]
                    info["strata_max_depth"] = max(float(b) for _, b, _ in strata if float(b) > 0)
            except Exception as e:
                info["pdf_parse_error"] = f"{type(e).__name__}: {e}"
        return info
    except Exception as e:
        return {"url": url, "error": f"{type(e).__name__}: {e}"}


@app.post("/lookup", response_model=LookupResponse)
def lookup(req: LookupRequest, x_api_key: Optional[str] = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")

    if not any([req.address, req.suburb, req.postcode,
                req.lat is not None and req.lng is not None]):
        raise HTTPException(
            status_code=400,
            detail="provide address, suburb, postcode, or lat+lng",
        )

    r = bdl.lookup(
        address=req.address,
        suburb=req.suburb,
        postcode=req.postcode,
        lat=req.lat,
        lng=req.lng,
        radius_km=req.radius_km,
        max_bores=req.max_bores,
    )

    return LookupResponse(
        depth=r.get("depth"),
        depth_p75=r.get("depth_p75"),
        depth_max=r.get("depth_max"),
        depth_range=r.get("depth_range"),
        aquifer=r.get("aquifer"),
        swl=r.get("swl"),
        yield_lps=r.get("yield_lps"),
        confidence=r.get("confidence", 0),
        confidence_label=r.get("confidence_label", "Very Low"),
        resolved_tier=(r.get("resolved") or {}).get("tier"),
        n_bores=len(r.get("bores", [])),
        errors=r.get("errors", []),
    )
