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

import hashlib
import hmac
import base64
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

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

    async def _request(self, path: str, body: Optional[Dict] = None) -> Any:
        """Make a signed POST request to the Solis Cloud API."""
        body = body or {}
        body_bytes = json.dumps(body).encode("utf-8")
        headers = self._sign(body_bytes, path)
        url = f"{self.base_url}{path}"

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, content=body_bytes, headers=headers)
            resp.raise_for_status()

        data = resp.json()

        # Solis API returns {"success": true, "code": "0", "data": {...}}
        if not data.get("success") and data.get("code") != "0":
            raise SolisCloudError(
                code=data.get("code", "unknown"),
                message=data.get("msg", "Unknown error"),
                data=data,
            )
        return data.get("data")

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
