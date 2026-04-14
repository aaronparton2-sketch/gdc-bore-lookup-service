# GDC Bore Depth Lookup Service

FastAPI wrapper around `bore-depth-lookup.py`. Designed for n8n inline calls.

## Endpoints

- `GET  /healthz` - liveness check (200 → OK)
- `POST /lookup`  - depth estimate. Header `X-API-Key: <key>` required if `LOOKUP_API_KEY` env var is set.

### POST /lookup body

```json
{
  "address": "1320 Dayboro Road, Rush Creek QLD",
  "suburb":  "Rush Creek",
  "postcode": "4521",
  "radius_km": 5,
  "max_bores": 10
}
```

(All four address fields optional - service falls back tier 1 → 2 → 3.)

### Response

```json
{
  "depth": 38,
  "depth_p75": 83,
  "depth_max": 126,
  "depth_range": "19-126m (n=10, ≤5km)",
  "aquifer": "Gubberamunda Sandstone",
  "swl": -55,
  "yield_lps": 8.0,
  "confidence": 55,
  "confidence_label": "Low",
  "resolved_tier": "full_address",
  "n_bores": 10,
  "errors": [],
  "attribution": "Bore data sourced from..."
}
```

## Deploy to Render (free tier)

1. Push this folder to a GitHub repo (public OK; no secrets in code).
2. In Render dashboard → **New +** → **Web Service** → connect the repo.
3. Render auto-detects `render.yaml`. Confirm:
   - Plan: **Free**
   - Build: `pip install -r requirements.txt`
   - Start: `uvicorn main:app --host 0.0.0.0 --port $PORT`
   - Health check: `/healthz`
4. **Add env var manually:** `LOOKUP_API_KEY` = a long random string (e.g. `openssl rand -hex 32`). Save the value - n8n needs it.
5. Click **Create Web Service**. First deploy takes 3-5 min.
6. Service URL will be `https://gdc-bore-lookup.onrender.com` (or similar).

## Cold start mitigation

Render free tier spins down after 15 min idle (cold start ~30-60 sec).

**Free fix:** set up a free cron at <https://cron-job.com> to GET `/healthz` every 10 minutes. Pre-warms the service so n8n doesn't hit a cold start.

## Local dev

```
pip install -r requirements.txt
uvicorn main:app --reload
# Then: curl -X POST http://localhost:8000/lookup -H 'Content-Type: application/json' -d '{"suburb":"Tara","postcode":"4421"}'
```

## Data source

Queensland Government Groundwater Database (CC BY 4.0).
Always attribute in any output that uses these estimates.
