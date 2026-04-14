"""
Bore Depth Lookup — QLD Groundwater Database

Given an address, suburb, or postcode, returns:
  - Likely drilled depth (median of nearby registered bores)
  - Depth range (min-max)
  - Dominant aquifer
  - Typical SWL + yield
  - Confidence % (0-100) reflecting address precision, bore count, variance, distance

Data sources (all public, CC BY 4.0, no auth):
  - ArcGIS:  spatial-gis.information.qld.gov.au .../GroundAndSurfaceWaterMonitoring/MapServer/1
  - Bore reports PDF:  resources.information.qld.gov.au/groundwater/reports/borereport

Usage:
    python bore-depth-lookup.py "1320 Dayboro Road, Rush Creek QLD"
    python bore-depth-lookup.py --suburb "Tara" --postcode 4421
    python bore-depth-lookup.py --lat -27.199 --lng 150.590
    python bore-depth-lookup.py --json "1320 Dayboro Road, Rush Creek QLD"
"""
import argparse
import hashlib
import io
import json
import math
import os
import re
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from statistics import median

import requests
from pypdf import PdfReader

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

ROOT = Path(__file__).resolve().parent.parent
GEOCODE_CACHE = ROOT / "outputs" / "geocode-cache.json"
BORE_CACHE = ROOT / "outputs" / "bore-report-cache.json"
PARSE_FAILURES = ROOT / "outputs" / "bore-parse-failures.log"

ARCGIS_QUERY = (
    "https://spatial-gis.information.qld.gov.au/arcgis/rest/services/"
    "InlandWaters/GroundAndSurfaceWaterMonitoring/MapServer/1/query"
)
BORE_REPORT_URL = (
    "https://resources.information.qld.gov.au/groundwater/reports/"
    "borereport?gw_pub_borecard&p_rn={rn}"
)
NOMINATIM = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "GrundyDrillingDepthLookup/1.0 (aaronparton2@gmail.com)"

ATTRIBUTION = (
    "Bore data sourced from Queensland Government Groundwater Database "
    "(CC BY 4.0). Estimate based on registered bores near the address. "
    "Guide only - not site-specific."
)


# ─── caches ───────────────────────────────────────────────────────────


def _load(path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


# ─── geocoding (3-tier resolver) ──────────────────────────────────────


def _nominatim(query, country="au"):
    """Single Nominatim query. Returns (lat, lng) or None."""
    headers = {"User-Agent": USER_AGENT}
    params = {"q": query, "format": "json", "limit": 1, "countrycodes": country}
    try:
        r = requests.get(NOMINATIM, headers=headers, params=params, timeout=15)
        r.raise_for_status()
        results = r.json()
        time.sleep(1.1)  # Nominatim ToS: max 1 req/sec
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        print(f"  [geocode error] {query!r}: {e}", file=sys.stderr)
    return None


def _cached_geocode(query):
    cache = _load(GEOCODE_CACHE)
    key = hashlib.md5(query.lower().strip().encode()).hexdigest()[:16]
    if key in cache:
        return tuple(cache[key]) if cache[key] else None
    result = _nominatim(query)
    cache[key] = list(result) if result else None
    _save(GEOCODE_CACHE, cache)
    return result


def resolve_location(address=None, suburb=None, postcode=None):
    """
    3-tier resolver. Returns (lat, lng, tier_name, address_score).
    Tier 1: full street address          → score 1.00
    Tier 2: suburb centroid              → score 0.70
    Tier 3: postcode centroid            → score 0.50
    """
    if address:
        cleaned = address.strip()
        if not re.search(r"\b(qld|queensland|nsw|aus|australia)\b", cleaned, re.I):
            cleaned += ", QLD, Australia"
        coords = _cached_geocode(cleaned)
        if coords:
            return (*coords, "full_address", 1.00)

    if suburb:
        q = f"{suburb}, QLD, Australia"
        coords = _cached_geocode(q)
        if coords:
            return (*coords, "suburb_centroid", 0.70)

    if postcode:
        q = f"{postcode}, Australia"
        coords = _cached_geocode(q)
        if coords:
            return (*coords, "postcode_centroid", 0.50)

    return None


# ─── ArcGIS: nearest bores ────────────────────────────────────────────


def haversine_km(lat1, lng1, lat2, lng2):
    R = 6371
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def query_nearby_bores(lat, lng, radius_km=10, max_bores=10):
    """
    Bbox query around (lat, lng). Returns list of dicts sorted by distance:
        [{rn, lat, lng, distance_km, drilled_date}, ...]

    Progressive search: start at given radius, expand if <3 found.
    """
    for r_km in (radius_km, radius_km * 2, radius_km * 4):
        if r_km > 40:
            break
        # crude bbox: 1 deg lat ≈ 111 km
        d_lat = r_km / 111.0
        d_lng = r_km / (111.0 * math.cos(math.radians(lat)))
        bbox = f"{lng - d_lng},{lat - d_lat},{lng + d_lng},{lat + d_lat}"

        params = {
            "f": "json",
            "where": "1=1",
            "geometry": bbox,
            "geometryType": "esriGeometryEnvelope",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "rn_char,gis_lat,gis_lng,drilled_date,facility_status_decode,facility_type_decode",
            "returnGeometry": "false",
            "inSR": "4326",
            "outSR": "4326",
            "resultRecordCount": 200,
        }
        try:
            resp = requests.get(ARCGIS_QUERY, params=params, timeout=30)
            resp.raise_for_status()
            features = resp.json().get("features", [])
        except Exception as e:
            print(f"  [arcgis error] radius {r_km}km: {e}", file=sys.stderr)
            features = []

        bores = []
        for f in features:
            a = f.get("attributes", {})
            rn = a.get("rn_char")
            blat, blng = a.get("gis_lat"), a.get("gis_lng")
            if not (rn and blat and blng):
                continue
            # skip abandoned/destroyed for depth estimation
            status = (a.get("facility_status_decode") or "").lower()
            if any(bad in status for bad in ("abandon", "destroyed", "filled")):
                continue
            # exclude non-water-supply bores (CSG/monitoring/investigation are
            # often 800m+ and skew water-bore depth estimates badly)
            ftype = (a.get("facility_type_decode") or "").lower()
            if any(bad in ftype for bad in (
                "csg", "monitoring", "investigation", "exploration",
                "mineral", "petroleum", "coal seam"
            )):
                continue
            d = haversine_km(lat, lng, blat, blng)
            if d > r_km:
                continue
            bores.append({
                "rn": rn,
                "lat": blat,
                "lng": blng,
                "distance_km": round(d, 2),
                "drilled_date": a.get("drilled_date"),
            })

        bores.sort(key=lambda b: b["distance_km"])

        if len(bores) >= 3 or r_km >= 40:
            return bores[:max_bores], r_km

    return [], radius_km


# ─── bore report PDF parsing ──────────────────────────────────────────

# Strata row pattern: "1 0.00 7.00 BROWN CLAY"
STRATA_ROW = re.compile(r"^\s*\d+\s+([\d.]+)\s+([\d.]+)\s+(.+?)$", re.MULTILINE)
# Aquifer row: "1 246.00 SDST - Sandstone 02/02/2004 -55.00 N COND 1600 8.00 Y PS GUBBERAMUNDA SANDSTONE"
AQUIFER_ROW = re.compile(
    r"^\s*\d+\s+([\d.]+)(?:\s+[\w-]+\s*-\s*[\w\s]+?)?\s+\d{2}/\d{2}/\d{4}"
    r"\s+(-?[\d.]+)?\s+\w+(?:\s+\w+)?\s+(?:[\d.]+\s+)?([\d.]+)\s+\w+\s+\w+\s+(.+?)$",
    re.MULTILINE,
)

# Known QLD/NSW aquifer formation names (fallback if structured row regex fails)
KNOWN_AQUIFERS = [
    "Gubberamunda Sandstone", "Hutton Sandstone", "Walloon Coal Measures",
    "Condamine Alluvium", "Precipice Sandstone", "Springbok Sandstone",
    "Mooga Sandstone", "Bungil Formation", "Orallo Formation",
    "Westbourne Formation", "Adori Sandstone", "Birkhead Formation",
    "Evergreen Formation", "Hooray Sandstone", "Cadna-owie Formation",
    "Toolebuc Formation", "Wallumbilla Formation", "Marburg Subgroup",
    "Main Range Volcanics", "Tertiary Basalt", "Tertiary Sediments",
    "Quaternary Alluvium", "Mary River Alluvium", "Lockyer Alluvium",
    "Brisbane Tuff", "Bundamba Group", "Esk Trough Sediments",
    "Helidon Sandstone", "Ipswich Coal Measures", "Burrum Coal Measures",
    "Maryborough Formation",
]


def fetch_bore_report(rn):
    """Returns (depth_m, aquifer_name, swl_m, yield_lps, drilled_date) or None."""
    cache = _load(BORE_CACHE)
    key = str(rn)
    if key in cache:
        c = cache[key]
        return tuple(c) if c else None

    url = BORE_REPORT_URL.format(rn=rn)
    try:
        r = requests.get(url, timeout=30)
        if not r.ok or len(r.content) < 500:
            cache[key] = None
            _save(BORE_CACHE, cache)
            return None
        reader = PdfReader(io.BytesIO(r.content))
        text = ""
        for page in reader.pages:
            text += page.extract_text() or ""
    except Exception as e:
        with PARSE_FAILURES.open("a", encoding="utf-8") as f:
            f.write(f"{rn}\tfetch_error\t{e}\n")
        cache[key] = None
        _save(BORE_CACHE, cache)
        return None

    # Drilled depth = max bottom of strata log
    depths = [float(b) for _, b, _ in STRATA_ROW.findall(text) if float(b) > 0]
    depth = max(depths) if depths else None

    # Aquifer block
    aquifer_name, swl, yield_lps = None, None, None
    aq_section = re.search(
        r"Aquifers.*?(?=Pump.Tests|Bore Conditions|Elevations|Water Analysis|$)",
        text, re.S,
    )
    if aq_section:
        for m in AQUIFER_ROW.finditer(aq_section.group(0)):
            try:
                top, swl_v, yld_v, name = m.groups()
                if not aquifer_name:
                    aquifer_name = name.strip().title()
                    if swl_v:
                        swl = float(swl_v)
                    yield_lps = float(yld_v)
            except Exception:
                continue

    # Fallback: scan whole text for known formation names
    if not aquifer_name:
        text_upper = text.upper()
        for fmt in KNOWN_AQUIFERS:
            if fmt.upper() in text_upper:
                aquifer_name = fmt
                break

    drilled_date = None
    dd = re.search(r"Drilled Date.*?(\d{2}/\d{2}/\d{4})", text)
    if dd:
        drilled_date = dd.group(1)

    if depth is None and aquifer_name is None:
        with PARSE_FAILURES.open("a", encoding="utf-8") as f:
            f.write(f"{rn}\tno_data_extracted\n")
        cache[key] = None
        _save(BORE_CACHE, cache)
        return None

    result = (depth, aquifer_name, swl, yield_lps, drilled_date)
    cache[key] = list(result)
    _save(BORE_CACHE, cache)
    return result


# ─── confidence scoring ───────────────────────────────────────────────


def confidence(address_score, bore_count, depths, median_distance_km):
    """0-100 confidence score per spec in plan."""
    if bore_count == 0 or not depths:
        return 0

    if bore_count >= 10:
        bcs = 1.00
    elif bore_count >= 5:
        bcs = 0.85
    elif bore_count >= 3:
        bcs = 0.70
    else:
        bcs = 0.50

    sorted_d = sorted(depths)
    n = len(sorted_d)
    q1 = sorted_d[n // 4] if n >= 4 else sorted_d[0]
    q3 = sorted_d[3 * n // 4] if n >= 4 else sorted_d[-1]
    iqr = q3 - q1
    med = median(sorted_d) or 1
    iqr_ratio = iqr / med
    if iqr_ratio < 0.20:
        vs = 1.00
    elif iqr_ratio < 0.50:
        vs = 0.80
    else:
        vs = 0.55

    if median_distance_km < 3:
        ds = 1.00
    elif median_distance_km < 7:
        ds = 0.85
    elif median_distance_km < 15:
        ds = 0.70
    else:
        ds = 0.50

    return round(address_score * bcs * vs * ds * 100)


def confidence_label(pct):
    if pct >= 80:
        return "High"
    if pct >= 60:
        return "Medium"
    if pct >= 40:
        return "Low"
    return "Very Low"


# ─── orchestrator ─────────────────────────────────────────────────────


def lookup(address=None, suburb=None, postcode=None, lat=None, lng=None,
           radius_km=5, max_bores=10):
    """Main entry. Returns dict with depth estimate + confidence."""
    result = {
        "input": {"address": address, "suburb": suburb, "postcode": postcode,
                  "lat": lat, "lng": lng},
        "resolved": None,
        "bores": [],
        "depth": None,
        "depth_p75": None,
        "depth_max": None,
        "depth_min": None,
        "depth_range": None,
        "aquifer": None,
        "swl": None,
        "yield_lps": None,
        "confidence": 0,
        "confidence_label": "Very Low",
        "attribution": ATTRIBUTION,
        "errors": [],
    }

    # 1. Resolve location
    if lat is not None and lng is not None:
        result["resolved"] = {"lat": lat, "lng": lng,
                              "tier": "explicit_coords", "address_score": 1.00}
    else:
        loc = resolve_location(address=address, suburb=suburb, postcode=postcode)
        if not loc:
            result["errors"].append("could not resolve location to lat/lng")
            return result
        rlat, rlng, tier, addr_score = loc
        result["resolved"] = {"lat": rlat, "lng": rlng,
                              "tier": tier, "address_score": addr_score}
        lat, lng = rlat, rlng

    addr_score = result["resolved"]["address_score"]

    # 2. ArcGIS nearby bores
    bores, used_radius = query_nearby_bores(lat, lng, radius_km, max_bores)
    if not bores:
        result["errors"].append(f"no registered bores within {used_radius}km")
        return result

    # 3. Parallel-fetch bore reports
    enriched = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        future_to_b = {ex.submit(fetch_bore_report, b["rn"]): b for b in bores}
        for fut in as_completed(future_to_b):
            b = future_to_b[fut]
            data = fut.result()
            if data and data[0]:
                depth, aq, swl, yld, drilled = data
                b.update({"depth_m": depth, "aquifer": aq, "swl": swl,
                          "yield_lps": yld, "drilled": drilled})
                enriched.append(b)

    if not enriched:
        result["errors"].append("found nearby bores but none had parseable depth data")
        result["bores"] = bores
        return result

    enriched.sort(key=lambda b: b["distance_km"])
    result["bores"] = enriched

    # 4. Aggregate
    depths = [b["depth_m"] for b in enriched if b.get("depth_m")]
    aquifers = [b["aquifer"] for b in enriched if b.get("aquifer")]
    swls = [b["swl"] for b in enriched if b.get("swl") is not None]
    yields = [b["yield_lps"] for b in enriched if b.get("yield_lps")]
    distances = [b["distance_km"] for b in enriched]

    result["depth"] = round(median(depths)) if depths else None
    if depths:
        sorted_d = sorted(depths)
        n = len(sorted_d)
        # P75 = upper quartile — use this for sales-friendly framing
        # (drillers prefer deeper jobs; underselling depth kills lead value)
        p75_idx = max(0, min(n - 1, int(n * 0.75)))
        result["depth_p75"] = round(sorted_d[p75_idx])
        result["depth_max"] = round(max(depths))
        result["depth_min"] = round(min(depths))
        result["depth_range"] = f"{round(min(depths))}-{round(max(depths))}m " \
                                f"(n={len(depths)}, ≤{used_radius}km)"
    if aquifers:
        result["aquifer"] = Counter(aquifers).most_common(1)[0][0]
    if swls:
        result["swl"] = round(median(swls))
    if yields:
        result["yield_lps"] = round(median(yields), 1)

    # 5. Confidence
    med_dist = median(distances) if distances else 999
    pct = confidence(addr_score, len(enriched), depths, med_dist)
    result["confidence"] = pct
    result["confidence_label"] = confidence_label(pct)

    return result


# ─── CLI ──────────────────────────────────────────────────────────────


def format_human(r):
    out = []
    inp = r["input"]
    out.append("=" * 60)
    out.append(f"INPUT: {inp.get('address') or inp.get('suburb') or inp.get('postcode') or 'lat/lng'}")
    if r["resolved"]:
        out.append(f"  Resolved via: {r['resolved']['tier']}  "
                   f"({r['resolved']['lat']:.4f}, {r['resolved']['lng']:.4f})")
    if r["errors"]:
        for e in r["errors"]:
            out.append(f"  ERROR: {e}")
    if r["depth"] is not None:
        out.append("")
        out.append(f"  Likely depth:    {r['depth']}m  ({r['depth_range']})")
        out.append(f"  Aquifer:         {r['aquifer'] or 'unknown'}")
        if r["swl"] is not None:
            out.append(f"  Typical SWL:     {r['swl']}m below surface")
        if r["yield_lps"]:
            out.append(f"  Typical yield:   {r['yield_lps']} L/s")
        out.append(f"  Confidence:      {r['confidence']}%  ({r['confidence_label']})")
    out.append("")
    out.append(f"  Attribution: {r['attribution']}")
    out.append("=" * 60)
    return "\n".join(out)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("address", nargs="?", help="Full address string")
    p.add_argument("--suburb")
    p.add_argument("--postcode")
    p.add_argument("--lat", type=float)
    p.add_argument("--lng", type=float)
    p.add_argument("--radius", type=float, default=5, help="Initial radius km (default 5)")
    p.add_argument("--max-bores", type=int, default=10)
    p.add_argument("--json", action="store_true", help="Output JSON instead of human-readable")
    args = p.parse_args()

    if not any([args.address, args.suburb, args.postcode,
                args.lat is not None and args.lng is not None]):
        p.error("provide an address, --suburb, --postcode, or --lat/--lng")

    r = lookup(address=args.address, suburb=args.suburb, postcode=args.postcode,
               lat=args.lat, lng=args.lng, radius_km=args.radius,
               max_bores=args.max_bores)

    if args.json:
        print(json.dumps(r, indent=2, default=str))
    else:
        print(format_human(r))


if __name__ == "__main__":
    main()
