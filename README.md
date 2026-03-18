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
