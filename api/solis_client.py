"""
Solis Cloud API v1 client.

Handles HMAC-SHA1 authentication and provides typed methods for
common Solis Cloud monitoring endpoints.

Docs: https://www.soliscloud.com (API section)
Base URL: https://www.soliscloud.com:13333

Authentication flow:
  1. Build canonical string: POST\n{Content-MD5}\napplication/json\n{Date}\n{Path}
  2. HMAC-SHA1 sign it with the API Secret
  3. Authorization header: API {KeyId}:{Signature}
"""

import asyncio
import hashlib
import hmac
import base64
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

log = logging.getLogger("solis_client")

SOLIS_BASE_URL = "https://www.soliscloud.com:13333"


class SolisCloudError(Exception):
    """Raised when the Solis Cloud API returns an error."""

    def __init__(self, code: str, message: str, data: Any = None):
        self.code = code
        self.message = message
        self.data = data
        super().__init__(f"SolisCloud [{code}]: {message}")


class SolisCloudClient:
    """Client for the Solis Cloud API v1."""

    def __init__(self, key_id: str, key_secret: str, base_url: str = SOLIS_BASE_URL):
        self.key_id = key_id
        self.key_secret = key_secret
        self.base_url = base_url.rstrip("/")
        # One HTTP client per event loop, created on first use (see _http()).
        self._http_client: Optional[httpx.AsyncClient] = None
        self._http_loop: Optional[asyncio.AbstractEventLoop] = None
        # Global request gate (see _gate()); also per loop.
        self._gate_lock: Optional[asyncio.Lock] = None
        self._gate_loop: Optional[asyncio.AbstractEventLoop] = None
        self._last_request_start = 0.0

    def _http(self) -> httpx.AsyncClient:
        """The shared HTTP client for the running event loop.

        Until 2026-09-25 every call opened its own AsyncClient — a fresh TCP +
        TLS handshake to soliscloud.com for each of the ~650 stationDay calls a
        sync run makes. Keep-alive connections remove that from each call. The
        client is bound to the loop that first used it: FastAPI has one loop
        for the life of the process, a script has one per asyncio.run(), and
        a second loop (a thread running its own asyncio.run) simply gets its
        own client rather than an "attached to a different loop" error.
        """
        loop = asyncio.get_running_loop()
        if self._http_client is None or self._http_loop is not loop:
            self._http_client = httpx.AsyncClient(
                timeout=30,
                limits=httpx.Limits(max_connections=32, max_keepalive_connections=32),
            )
            self._http_loop = loop
        return self._http_client

    async def aclose(self) -> None:
        """Close the shared client. Scripts call this at the end of a run;
        leaving it open is harmless (connections close with the process)."""
        if self._http_client is not None:
            try:
                await self._http_client.aclose()
            except Exception:
                pass
            self._http_client = None
            self._http_loop = None

    async def _gate(self) -> None:
        """Space request STARTS at least _MIN_INTERVAL apart across all
        concurrent tasks, so raising SOLIS_CONCURRENCY in a caller can never
        push the account past ~10 requests/s. The old per-task sleep after a
        success only bounded each task, not the sum."""
        loop = asyncio.get_running_loop()
        if self._gate_lock is None or self._gate_loop is not loop:
            self._gate_lock = asyncio.Lock()
            self._gate_loop = loop
        async with self._gate_lock:
            now = time.monotonic()
            wait = self._last_request_start + self._MIN_INTERVAL - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self._last_request_start = now

    def _sign(self, body: bytes, path: str) -> Dict[str, str]:
        """Build the signed headers for a Solis Cloud API request."""
        content_md5 = base64.b64encode(
            hashlib.md5(body).digest()
        ).decode("utf-8")
        content_type = "application/json"
        date = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

        # Canonical string: POST\n{MD5}\n{Content-Type}\n{Date}\n{Path}
        sign_str = f"POST\n{content_md5}\n{content_type}\n{date}\n{path}"
        signature = base64.b64encode(
            hmac.new(
                self.key_secret.encode("utf-8"),
                sign_str.encode("utf-8"),
                hashlib.sha1,
            ).digest()
        ).decode("utf-8")

        return {
            "Content-Type": content_type,
            "Content-MD5": content_md5,
            "Date": date,
            "Authorization": f"API {self.key_id}:{signature}",
        }

    # Rate-limit: max 10 requests per second ACROSS the process (see _gate),
    # with retry + exponential backoff.
    _MIN_INTERVAL = 0.1       # 100ms between request starts (~10 req/s)
    _MAX_RETRIES = 3
    _BACKOFF_BASE = 2.0       # seconds

    # Application-level errors (code != '0') are retried separately from HTTP
    # 5xx/429, and far more cheaply. They must be retried at all, because code
    # '1' covers genuine transient outages — but the SAME code is returned for
    # a station that does not exist, and those never recover. Using the 2/4/8s
    # HTTP backoff here would spend 14s per unknown station: the last full
    # backfill logged 1,084 errors over 4,583 calls, which at SOLIS_CONCURRENCY=2
    # would have added roughly two hours to a 140-minute run. One quick retry
    # catches the transients (which nearly always clear immediately) without
    # paying that on every dead station.
    _API_ERROR_RETRIES = 1
    _API_ERROR_DELAY = 1.5    # seconds

    async def _request(self, path: str, body: Optional[Dict] = None) -> Any:
        """Make a signed POST request to the Solis Cloud API with retry logic."""
        body = body or {}
        body_bytes = json.dumps(body).encode("utf-8")
        url = f"{self.base_url}{path}"

        last_exc: Optional[Exception] = None
        for attempt in range(1, self._MAX_RETRIES + 1):
            # Re-sign each attempt (Date header must be fresh)
            headers = self._sign(body_bytes, path)

            try:
                await self._gate()
                resp = await self._http().post(url, content=body_bytes, headers=headers)

                # Rate-limited (429) or server error (5xx) → retry
                if resp.status_code == 429 or resp.status_code >= 500:
                    wait = self._BACKOFF_BASE ** attempt
                    log.warning(
                        "Solis %s returned %d, retrying in %.1fs (attempt %d/%d)",
                        path, resp.status_code, wait, attempt, self._MAX_RETRIES,
                    )
                    last_exc = httpx.HTTPStatusError(
                        f"{resp.status_code}", request=resp.request, response=resp,
                    )
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()

            except httpx.TimeoutException as e:
                wait = self._BACKOFF_BASE ** attempt
                log.warning(
                    "Solis %s timed out, retrying in %.1fs (attempt %d/%d)",
                    path, wait, attempt, self._MAX_RETRIES,
                )
                last_exc = e
                await asyncio.sleep(wait)
                continue

            data = resp.json()

            # Solis signals success as {"success": true, "code": "0", "data": {...}}.
            #
            # This guard used to be `and`:
            #     if not data.get("success") and data.get("code") != "0":
            # which never fired for the most common failure, because Solis
            # returns success=true ALONGSIDE code='1'. Measured live against
            # /v1/api/stationDetail, all HTTP 200:
            #     valid id     -> success=True  code='0' data={...}
            #     unknown id   -> success=True  code='1' data=null
            #     garbage id   -> success=True  code='1' data=null
            #     empty id     -> success=False code='1' data=''
            # `not True` is False, so the two middle rows short-circuited the
            # whole condition and fell through to `return data.get("data")`,
            # i.e. None. Callers could not distinguish a failed call from an
            # empty result, so a Solis outage presented as "this station does
            # not exist" — precisely the error most likely to make someone
            # delete a correct station mapping.
            #
            # code '1' is irreducibly ambiguous: its msg is "Communication
            # error. Please refresh and try again later" for BOTH an unknown
            # station and a genuine outage, and the two payloads are identical
            # byte for byte. Solis gives us no way to separate them, so we do
            # not pretend to. It is retried like a 5xx and then raised.
            #
            # NEVER infer that a station does not exist from this error. To
            # answer that question, check the account's station roster
            # (userStationList / list_stations), which is authoritative.
            if not data.get("success") or str(data.get("code")) != "0":
                api_exc = SolisCloudError(
                    code=str(data.get("code", "unknown")),
                    message=data.get("msg", "Unknown error"),
                    data=data,
                )
                if attempt <= self._API_ERROR_RETRIES:
                    log.warning(
                        "Solis %s returned code=%s (%s), retrying in %.1fs (attempt %d/%d)",
                        path, api_exc.code, api_exc.message[:60],
                        self._API_ERROR_DELAY, attempt, self._API_ERROR_RETRIES + 1,
                    )
                    last_exc = api_exc
                    await asyncio.sleep(self._API_ERROR_DELAY)
                    continue
                raise api_exc

            return data.get("data")

        # All retries exhausted
        raise last_exc or RuntimeError(f"Solis request to {path} failed after {self._MAX_RETRIES} retries")

    # ------------------------------------------------------------------
    # Station endpoints
    # ------------------------------------------------------------------

    async def list_stations(self, page_no: int = 1, page_size: int = 20) -> Dict:
        """List all stations for the account."""
        return await self._request("/v1/api/userStationList", {
            "pageNo": page_no,
            "pageSize": page_size,
        })

    async def station_detail(self, station_id: str) -> Dict:
        """Get detailed info for a specific station."""
        return await self._request("/v1/api/stationDetail", {
            "id": station_id,
        })

    async def station_day(self, station_id: str, date_str: str, currency: str = "PHP") -> Dict:
        """Get station daily generation data.

        Args:
            station_id: Station ID
            date_str: Date in YYYY-MM-DD format
            currency: Currency code for income calculation
        """
        return await self._request("/v1/api/stationDay", {
            "id": station_id,
            "money": currency,
            "time": date_str,
            "timeZone": 8,  # PHT (UTC+8)
        })

    async def station_month(self, station_id: str, month_str: str, currency: str = "PHP") -> Dict:
        """Get station monthly generation data.

        Args:
            station_id: Station ID
            month_str: Month in YYYY-MM format
        """
        return await self._request("/v1/api/stationMonth", {
            "id": station_id,
            "money": currency,
            "month": month_str,
            "timeZone": 8,
        })

    async def station_year(self, station_id: str, year: str, currency: str = "PHP") -> Dict:
        """Get station yearly generation data."""
        return await self._request("/v1/api/stationYear", {
            "id": station_id,
            "money": currency,
            "year": year,
            "timeZone": 8,
        })

    async def station_all(self, station_id: str, currency: str = "PHP") -> Dict:
        """Get station all-time generation data."""
        return await self._request("/v1/api/stationAll", {
            "id": station_id,
            "money": currency,
            "timeZone": 8,
        })

    # ------------------------------------------------------------------
    # Inverter endpoints
    # ------------------------------------------------------------------

    async def list_inverters(self, station_id: str, page_no: int = 1, page_size: int = 20) -> Dict:
        """List all inverters for a station."""
        return await self._request("/v1/api/inverterList", {
            "stationId": station_id,
            "pageNo": page_no,
            "pageSize": page_size,
        })

    async def inverter_detail(self, inverter_id: str) -> Dict:
        """Get real-time data for a specific inverter."""
        return await self._request("/v1/api/inverterDetail", {
            "id": inverter_id,
        })

    async def inverter_detail_list(self, inverter_id: str) -> Dict:
        """Get detailed real-time data list for a specific inverter."""
        return await self._request("/v1/api/inverterDetailList", {
            "id": inverter_id,
        })

    async def inverter_day(self, inverter_id: str, date_str: str, currency: str = "PHP") -> Dict:
        """Get inverter daily power curve data.

        Args:
            inverter_id: Inverter ID (sn)
            date_str: Date in YYYY-MM-DD format
        """
        return await self._request("/v1/api/inverterDay", {
            "id": inverter_id,
            "money": currency,
            "time": date_str,
            "timeZone": 8,
        })

    async def inverter_month(self, inverter_id: str, month_str: str, currency: str = "PHP") -> Dict:
        """Get inverter monthly generation data."""
        return await self._request("/v1/api/inverterMonth", {
            "id": inverter_id,
            "money": currency,
            "month": month_str,
            "timeZone": 8,
        })

    async def inverter_year(self, inverter_id: str, year: str, currency: str = "PHP") -> Dict:
        """Get inverter yearly generation data."""
        return await self._request("/v1/api/inverterYear", {
            "id": inverter_id,
            "money": currency,
            "year": year,
            "timeZone": 8,
        })

    async def inverter_all(self, inverter_id: str, currency: str = "PHP") -> Dict:
        """Get inverter all-time generation data."""
        return await self._request("/v1/api/inverterAll", {
            "id": inverter_id,
            "money": currency,
            "timeZone": 8,
        })

    # ------------------------------------------------------------------
    # Alarm endpoints
    # ------------------------------------------------------------------

    async def alarm_list(
        self,
        station_id: str,
        page_no: int = 1,
        page_size: int = 20,
        begin_time: Optional[str] = None,
        end_time: Optional[str] = None,
    ) -> Dict:
        """List alarms for a station.

        Args:
            station_id: Station ID
            begin_time: Start time in YYYY-MM-DD HH:MM:SS format (optional)
            end_time: End time in YYYY-MM-DD HH:MM:SS format (optional)
        """
        body: Dict[str, Any] = {
            "stationId": station_id,
            "pageNo": page_no,
            "pageSize": page_size,
        }
        if begin_time:
            body["beginTime"] = begin_time
        if end_time:
            body["endTime"] = end_time
        return await self._request("/v1/api/alarmList", body)

    # ------------------------------------------------------------------
    # Collector / Data Logger endpoints
    # ------------------------------------------------------------------

    async def list_collectors(self, station_id: str, page_no: int = 1, page_size: int = 20) -> Dict:
        """List data loggers/collectors for a station."""
        return await self._request("/v1/api/collectorList", {
            "stationId": station_id,
            "pageNo": page_no,
            "pageSize": page_size,
        })

    async def collector_detail(self, collector_sn: str) -> Dict:
        """Get collector detail by serial number."""
        return await self._request("/v1/api/collectorDetail", {
            "id": collector_sn,
        })
