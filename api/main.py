"""
Solviva Monitoring API — lightweight backend for the Solar Dashboard.

Proxies authenticated requests to the Solis Cloud API.
Designed to deploy on Render / Railway / Fly.io free tier.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
import os

from dotenv import load_dotenv

load_dotenv()

# Import routes
from .solis_client import SolisCloudClient  # noqa: F401 — used by routes
from .solis_routes import router as solis_router
from .app_routes import router as app_router
from .validation_routes import router as validation_router
from .monitoring_admin_routes import router as monitoring_admin_router

app = FastAPI(title="Solviva Monitoring API", docs_url="/docs")

# Browser origins allowed to call this API.
#
# The "*" entry was removed with the Monitoring Admin routes. It was commented
# "Mobile app (Expo/React Native)", but React Native's fetch does not enforce
# CORS at all — native apps were never relying on it, and it made every
# endpoint callable from any web page on the internet.
#
# allow_credentials stays False: Monitoring Admin authenticates with a bearer
# token in the Authorization header, which browsers do not attach automatically,
# so CSRF is not reachable regardless of origin. Do NOT set it True alongside a
# wildcard origin — Starlette then reflects the caller's origin verbatim.
ALLOWED_ORIGINS = [
    "https://solvivaenergy.github.io",
    os.getenv("MONITORING_ADMIN_ORIGIN", "https://monitoring.solvivaenergy.com"),
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "http://localhost:5173",   # Vite dev server for Monitoring Admin
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    # POST is required by /admin/validate/*; without it the browser preflight
    # fails and every validate button is dead.
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["authorization", "content-type", "apikey"],
    allow_credentials=False,
    max_age=600,
)

app.include_router(solis_router)
app.include_router(app_router)
app.include_router(validation_router)
# Monitoring Admin lives at /monitoring-admin on this same service: no second
# host, no CORS, one deploy. Staff-only via Supabase JWT + staff_users; see the
# module.
app.include_router(monitoring_admin_router)


# It was called "back office" and served at /backoffice until 2026-09-18.
# Bookmarks, the Supabase Site URL and any still-open copy of the old page keep
# working: 308 preserves the method and body (so the old page's API calls land),
# the query string is carried over (Supabase's ?code= PKCE return), and
# browsers carry the #fragment (implicit-flow tokens) across a redirect.
@app.api_route("/backoffice", methods=["GET", "POST", "PATCH", "DELETE"], include_in_schema=False)
@app.api_route("/backoffice/{rest:path}", methods=["GET", "POST", "PATCH", "DELETE"], include_in_schema=False)
async def legacy_backoffice_redirect(request: Request, rest: str = ""):
    target = "/monitoring-admin" + (f"/{rest}" if rest else "")
    if request.url.query:
        target += "?" + request.url.query
    return RedirectResponse(target, status_code=308)


@app.get("/health")
async def health():
    return {"status": "ok"}
