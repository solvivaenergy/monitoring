"""Instance health of the Supabase project, from its own metrics endpoint.

GET https://<ref>.supabase.co/customer/v1/privileged/metrics with HTTP basic
auth ``service_role:<service key>`` returns the node_exporter and
postgres_exporter counters the dashboard charts are drawn from (~600 KB of
Prometheus text). Only a handful are read here: memory, swap, load, the data
disk and the database/WAL size.

Why memory and swap rather than disk IO: Supabase's "running out of Disk IO
Budget" mail of 2026-09-23 was the Nano compute swapping ~400 MB around the
clock. Since the move to Small, disk traffic has been ~1% of the tier's
baseline (measured 2026-10-10) while the hot data set grows with the fleet —
so free memory shrinking and swap filling are the leading indicators, and
they are visible here minutes after they start instead of in an e-mail days
later. Used by the worker's health task (alerts) and by Monitoring Admin's
Health tab (display).

The endpoint is Supabase-specific and may change; every caller treats a
failure as "no reading", never as an error of its own.
"""
from __future__ import annotations

import os
import re
import time
from typing import Dict, Optional

import httpx

DEFAULT_MEM_AVAILABLE_MIN_MB = 300
DEFAULT_SWAP_USED_MAX_MB = 300

_WANTED = (
    "node_memory_MemTotal_bytes", "node_memory_MemAvailable_bytes",
    "node_memory_SwapTotal_bytes", "node_memory_SwapFree_bytes",
    "node_load1", "node_filesystem_avail_bytes", "node_filesystem_size_bytes",
    "pg_database_size_mb", "pg_wal_size_mb",
)
_LINE = re.compile(r"^(" + "|".join(_WANTED) + r")(\{[^}]*\})?\s+(\S+)")


def thresholds_from_env() -> Dict[str, int]:
    return {
        "mem_available_min_mb": int(os.getenv("INSTANCE_MEM_AVAILABLE_MIN_MB", str(DEFAULT_MEM_AVAILABLE_MIN_MB))),
        "swap_used_max_mb": int(os.getenv("INSTANCE_SWAP_USED_MAX_MB", str(DEFAULT_SWAP_USED_MAX_MB))),
    }


def parse_instance_metrics(text: str) -> Dict[str, float]:
    """The few readings we care about, in MB / GB (decimal, like the dashboard).

    Filesystem figures are the /data volume (Postgres lives there; the root
    volume is the OS). Database and WAL sizes take the largest series when the
    exporter reports several databases.
    """
    raw: Dict[str, float] = {}
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        m = _LINE.match(line)
        if not m:
            continue
        name, labels, value = m.group(1), m.group(2) or "", m.group(3)
        if name.startswith("node_filesystem_") and 'mountpoint="/data"' not in labels:
            continue
        try:
            v = float(value)
        except ValueError:
            continue
        if name in ("pg_database_size_mb", "pg_wal_size_mb"):
            raw[name] = max(raw.get(name, 0.0), v)
        else:
            raw[name] = v

    def mb(key: str) -> Optional[int]:
        return None if key not in raw else int(round(raw[key] / 1e6))

    out: Dict[str, float] = {}
    if "node_memory_MemTotal_bytes" in raw:
        out["mem_total_mb"] = mb("node_memory_MemTotal_bytes")
        out["mem_available_mb"] = mb("node_memory_MemAvailable_bytes")
    if "node_memory_SwapTotal_bytes" in raw:
        out["swap_total_mb"] = mb("node_memory_SwapTotal_bytes")
        out["swap_used_mb"] = int(round((raw["node_memory_SwapTotal_bytes"] - raw.get("node_memory_SwapFree_bytes", 0.0)) / 1e6))
    if "node_load1" in raw:
        out["load1"] = round(raw["node_load1"], 2)
    if "node_filesystem_size_bytes" in raw:
        out["data_disk_total_gb"] = round(raw["node_filesystem_size_bytes"] / 1e9, 1)
        out["data_disk_free_gb"] = round(raw.get("node_filesystem_avail_bytes", 0.0) / 1e9, 1)
    if "pg_database_size_mb" in raw:
        out["db_size_mb"] = int(raw["pg_database_size_mb"])
    if "pg_wal_size_mb" in raw:
        out["wal_size_mb"] = int(raw["pg_wal_size_mb"])
    out["fetched_at"] = time.time()
    return out


def fetch_instance_metrics(supabase_url: str, service_key: str, timeout: float = 20.0) -> Dict[str, float]:
    """One scrape, parsed. Raises on HTTP/network errors; callers decide."""
    url = supabase_url.rstrip("/") + "/customer/v1/privileged/metrics"
    with httpx.Client(timeout=timeout) as client:
        resp = client.get(url, auth=("service_role", service_key))
        resp.raise_for_status()
    metrics = parse_instance_metrics(resp.text)
    if "mem_available_mb" not in metrics:
        raise RuntimeError("metrics endpoint answered without memory counters")
    return metrics


def pressure_message(metrics: Dict[str, float], mem_available_min_mb: int, swap_used_max_mb: int) -> Optional[str]:
    """A one-line description of the breach, or None when the instance is fine."""
    problems = []
    avail = metrics.get("mem_available_mb")
    if avail is not None and avail < mem_available_min_mb:
        problems.append(f"{avail:.0f} MB free of {metrics.get('mem_total_mb', 0):.0f} MB (threshold {mem_available_min_mb} MB)")
    swap = metrics.get("swap_used_mb")
    if swap is not None and swap > swap_used_max_mb:
        problems.append(f"swap in use {swap:.0f} MB (threshold {swap_used_max_mb} MB)")
    return "; ".join(problems) or None
