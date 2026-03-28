"""
Solviva Monitoring API — lightweight backend for the Solar Dashboard.

Proxies authenticated requests to the Solis Cloud API.
Designed to deploy on Render / Railway / Fly.io free tier.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os

from dotenv import load_dotenv

load_dotenv()

# Import routes
from .solis_client import SolisCloudClient  # noqa: F401 — used by routes
from .solis_routes import router as solis_router
from .app_routes import router as app_router

app = FastAPI(title="Solviva Monitoring API", docs_url="/docs")

# Allow the GitHub Pages frontend and mobile app to call this API
ALLOWED_ORIGINS = [
    "https://solvivaenergy.github.io",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "*",  # Mobile app (Expo/React Native)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(solis_router)
app.include_router(app_router)


@app.get("/health")
async def health():
    return {"status": "ok"}
