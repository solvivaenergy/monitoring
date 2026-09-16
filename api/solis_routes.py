"""
FastAPI routes for Solis Cloud API integration.

All endpoints require SOLIS_CLOUD_KEY_ID and SOLIS_CLOUD_KEY_SECRET
to be set in the .env file.
"""

import hmac
import logging
import os
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query

from .solis_client import SolisCloudClient, SolisCloudError

log = logging.getLogger(__name__)


async def _require_token(authorization: str = Header(default="")) -> None:
    """Gate every /solis/* route behind a shared token.

    These 16 routes had no authentication of any kind. Verified live on
    2026-09-15: an anonymous GET to /solis/stations returned HTTP 200 with the
    whole fleet — 648 stations, every customer's name, station id, installer and
    data-logger serial. They proxy our Solis credentials, so they are effectively
    a public read API over the entire customer base.

    A shared token is the minimum that closes it without rewriting the callers.
    It is NOT per-user authorisation: anyone holding the token still sees the
    whole fleet, so it must not be embedded in the customer-facing dashboard or
    the mobile bundle. Customer-scoped data belongs on /app/*, which
    authenticates the Supabase JWT and resolves that user's own stations.

    Set SOLIS_API_TOKEN in the Render environment. If it is unset the routes
    refuse to serve rather than falling open — an unset secret must never mean
    "no authentication required".
    """
    expected = os.getenv("SOLIS_API_TOKEN", "")
    if not expected:
        log.error("SOLIS_API_TOKEN is not configured; refusing all /solis/* requests")
        raise HTTPException(
            status_code=503,
            detail="This endpoint is not configured for access.",
        )

    supplied = ""
    if authorization.startswith("Bearer "):
        supplied = authorization[7:]

    # compare_digest, not ==, so a wrong token cannot be recovered by timing.
    if not supplied or not hmac.compare_digest(supplied, expected):
        raise HTTPException(status_code=401, detail="Unauthorized")


router = APIRouter(
    prefix="/solis",
    tags=["Solis Cloud"],
    dependencies=[Depends(_require_token)],
)


def _get_client() -> SolisCloudClient:
    key_id = os.getenv("SOLIS_CLOUD_KEY_ID", "")
    key_secret = os.getenv("SOLIS_CLOUD_KEY_SECRET", "")
    if not key_id or not key_secret:
        raise HTTPException(
            status_code=500,
            detail="SOLIS_CLOUD_KEY_ID and SOLIS_CLOUD_KEY_SECRET must be configured in .env",
        )
    return SolisCloudClient(key_id, key_secret)


async def _call(coro):
    """Wrap Solis API calls with error handling."""
    try:
        return await coro
    except SolisCloudError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================================
# Station endpoints
# ======================================================================

@router.get("/stations")
async def list_stations(
    page_no: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """List all stations in the Solis Cloud account."""
    client = _get_client()
    return await _call(client.list_stations(page_no, page_size))


@router.get("/stations/{station_id}")
async def station_detail(station_id: str):
    """Get detailed info for a specific station."""
    client = _get_client()
    return await _call(client.station_detail(station_id))


@router.get("/stations/{station_id}/day")
async def station_day(
    station_id: str,
    date_str: str = Query(
        default=None,
        alias="date",
        description="Date in YYYY-MM-DD format (defaults to today)",
    ),
):
    """Get station daily generation data (power curve)."""
    if not date_str:
        date_str = date.today().isoformat()
    client = _get_client()
    return await _call(client.station_day(station_id, date_str))


@router.get("/stations/{station_id}/month")
async def station_month(
    station_id: str,
    month_str: str = Query(
        default=None,
        alias="month",
        description="Month in YYYY-MM format (defaults to current month)",
    ),
):
    """Get station monthly generation data."""
    if not month_str:
        month_str = date.today().strftime("%Y-%m")
    client = _get_client()
    return await _call(client.station_month(station_id, month_str))


@router.get("/stations/{station_id}/year")
async def station_year(
    station_id: str,
    year: str = Query(
        default=None,
        description="Year in YYYY format (defaults to current year)",
    ),
):
    """Get station yearly generation data."""
    if not year:
        year = str(date.today().year)
    client = _get_client()
    return await _call(client.station_year(station_id, year))


@router.get("/stations/{station_id}/all")
async def station_all(station_id: str):
    """Get station all-time generation data."""
    client = _get_client()
    return await _call(client.station_all(station_id))


# ======================================================================
# Inverter endpoints
# ======================================================================

@router.get("/stations/{station_id}/inverters")
async def list_inverters(
    station_id: str,
    page_no: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """List all inverters for a station."""
    client = _get_client()
    return await _call(client.list_inverters(station_id, page_no, page_size))


@router.get("/inverters/{inverter_id}")
async def inverter_detail(inverter_id: str):
    """Get real-time data for a specific inverter."""
    client = _get_client()
    return await _call(client.inverter_detail(inverter_id))


@router.get("/inverters/{inverter_id}/detail-list")
async def inverter_detail_list(inverter_id: str):
    """Get detailed real-time data list for an inverter."""
    client = _get_client()
    return await _call(client.inverter_detail_list(inverter_id))


@router.get("/inverters/{inverter_id}/day")
async def inverter_day(
    inverter_id: str,
    date_str: str = Query(
        default=None,
        alias="date",
        description="Date in YYYY-MM-DD format (defaults to today)",
    ),
):
    """Get inverter daily power curve data."""
    if not date_str:
        date_str = date.today().isoformat()
    client = _get_client()
    return await _call(client.inverter_day(inverter_id, date_str))


@router.get("/inverters/{inverter_id}/month")
async def inverter_month(
    inverter_id: str,
    month_str: str = Query(
        default=None,
        alias="month",
        description="Month in YYYY-MM format",
    ),
):
    """Get inverter monthly generation data."""
    if not month_str:
        month_str = date.today().strftime("%Y-%m")
    client = _get_client()
    return await _call(client.inverter_month(inverter_id, month_str))


@router.get("/inverters/{inverter_id}/year")
async def inverter_year(
    inverter_id: str,
    year: str = Query(default=None, description="Year in YYYY format"),
):
    """Get inverter yearly generation data."""
    if not year:
        year = str(date.today().year)
    client = _get_client()
    return await _call(client.inverter_year(inverter_id, year))


@router.get("/inverters/{inverter_id}/all")
async def inverter_all(inverter_id: str):
    """Get inverter all-time generation data."""
    client = _get_client()
    return await _call(client.inverter_all(inverter_id))


# ======================================================================
# Alarm endpoints
# ======================================================================

@router.get("/stations/{station_id}/alarms")
async def alarm_list(
    station_id: str,
    page_no: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    begin_time: Optional[str] = Query(None, description="Start: YYYY-MM-DD HH:MM:SS"),
    end_time: Optional[str] = Query(None, description="End: YYYY-MM-DD HH:MM:SS"),
):
    """List alarms for a station."""
    client = _get_client()
    return await _call(client.alarm_list(station_id, page_no, page_size, begin_time, end_time))


# ======================================================================
# Collector / Data Logger endpoints
# ======================================================================

@router.get("/stations/{station_id}/collectors")
async def list_collectors(
    station_id: str,
    page_no: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """List data loggers/collectors for a station."""
    client = _get_client()
    return await _call(client.list_collectors(station_id, page_no, page_size))


@router.get("/collectors/{collector_sn}")
async def collector_detail(collector_sn: str):
    """Get collector detail by serial number."""
    client = _get_client()
    return await _call(client.collector_detail(collector_sn))
