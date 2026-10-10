# Solviva Solar Monitoring Dashboard

Live solar monitoring dashboard for Solviva Energy's Solis Cloud inverters.

**Dashboard**: https://solvivaenergy.github.io/monitoring

## Architecture

```
GitHub Pages (static)          Render (API backend)           Solis Cloud
┌────────────────────┐        ┌────────────────────┐        ┌──────────────┐
│  index.html        │──GET──▶│  FastAPI            │──POST─▶│  soliscloud   │
│  (browser)         │◀──JSON─│  /solis/*           │◀──JSON─│  :13333 API  │
└────────────────────┘        └────────────────────┘        └──────────────┘
                               HMAC-SHA1 signed requests
```

- **Frontend** — Static HTML/JS hosted on GitHub Pages (free)
- **Backend** — FastAPI proxy on Render free tier, signs requests with Solis API credentials
- **Solis Cloud** — Solar inverter monitoring API at `soliscloud.com:13333`

## Setup

### 1. Create the repo

```bash
cd monitoring
git init
git remote add origin https://github.com/solvivaenergy/monitoring.git
git add .
git commit -m "Initial dashboard"
git push -u origin main
```

### 2. Enable GitHub Pages

1. Go to **Settings → Pages** in the repo
2. Set **Source** to **GitHub Actions**
3. The `pages.yml` workflow will auto-deploy on push

### 3. Deploy the API backend (Render)

1. Go to [render.com](https://render.com) → **New Web Service**
2. Connect the `solvivaenergy/monitoring` repo
3. Set **Root Directory** to `.` (root)
4. It will detect the `render.yaml` — or manually set:
   - **Build Command**: `pip install -r api/requirements.txt`
   - **Start Command**: `uvicorn api.main:app --host 0.0.0.0 --port $PORT`
5. Add environment variables:
   - `SOLIS_CLOUD_KEY_ID` = `1300386381677211099`
   - `SOLIS_CLOUD_KEY_SECRET` = _(your secret key)_

### 4. Update the dashboard API URL

After deploying on Render, you'll get a URL like `https://solviva-api.onrender.com`.

Edit `index.html` line where `API_BASE` is defined:

```js
const API_BASE = "https://solviva-api.onrender.com";
```

## Local Development

```bash
# Start the API backend locally
cd monitoring
pip install -r api/requirements.txt
# Create .env with SOLIS_CLOUD_KEY_ID and SOLIS_CLOUD_KEY_SECRET
uvicorn api.main:app --port 8000

# Open index.html (update API_BASE to http://localhost:8000)
```

## Files

| Path                          | Purpose                                 |
| ----------------------------- | --------------------------------------- |
| `index.html`                  | Dashboard frontend (GitHub Pages)       |
| `assets/`                     | Logo, favicon                           |
| `api/main.py`                 | FastAPI app with CORS for GitHub Pages  |
| `api/solis_client.py`         | Solis Cloud API client (HMAC-SHA1 auth) |
| `api/solis_routes.py`         | `/solis/*` REST endpoints               |
| `render.yaml`                 | Render.com deployment config            |
| `.github/workflows/pages.yml` | GitHub Pages auto-deploy                |

## Scaling checkpoints (noted 2026-10-10)

Capacity plan for the +2,500-installation target (fleet 710 → ~3,200). The
2026-10-10 change set shipped the fixes that do not depend on fleet size
(watermark view, lifetime slices, nightly on `stationDayEnergyList` with a
7-day window, chunked five-minute pass, memory alerts on the Health tab,
backfill pause knob). Three more are deliberately deferred — **revisit them
when the fleet reaches ~1,500 stations**, which is before any of them bites:

1. **Fetch strategy** — a full stationDay pass is Solis-bound at ~3.5 calls/s
   (6–15 % of calls answered 429 at 700 stations); the 15-minute cadence stops
   fitting at ~2,000 stations. Plan: full curve pass every 30 min, the hot
   pass (viewed stations) stays at 5 min, plus the roster heartbeat below if
   fleet-wide freshness is wanted. Nothing is lost in between — stationDay
   returns the whole day each time — only freshness slips.
2. **Roster heartbeat** (product decision) — `userStationList` every 5 min
   (7 calls today, 32 at 3,200) carries `power`, `dataTimestamp`, `state` and
   today's cumulative energies: one provisional "latest point" per station,
   overwritten by the next full pass through the existing upsert. Also gives
   every station's online state every 5 min (dark-station detection).
3. **Monitoring Admin per-station stats** — the grid's grouped scans over the
   reading tables grow with the hourly table; a nightly `station_stats` table
   makes the grid one small read.

Triggers, visible on the Health tab: fleet pass > 8 min → item 1; database
free memory < 300 MB or swap > 300 MB → Supabase compute Medium ($60/mo);
grid > 5 s → item 3; Solis 429 rate > 20 % → ask Solis about quota/keys.
Measured headroom on 2026-10-10: disk IO ~1 % of the Small tier's baseline;
RAM is the first database limit, around 1,500–2,000 stations.
