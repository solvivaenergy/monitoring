"""Solis Real-time Data Forwarding — the MQTT capture task (step 3 of the
forwarding plan, 2026-10-13).

Solis publishes one JSON message per data-logger upload to the MQTT broker we
host (EMQX Cloud Serverless, TLS on 8883; our subscriber login is
EMQX_WORKER_USER). Per Solis's integration guide the message carries
``time`` (ms epoch), ``sn`` (collector serial), ``isRealtime`` (0 = live,
1 = historical backfill after a reconnect), ``collector1`` (the logger) and
``inverter1`` (every inverter under it, with pac, eToday/eTotal, grid,
battery, load, state and alarms).

What this task does, and deliberately nothing more: keep every message
verbatim in public.solis_forward_messages (migration 26) so the test
collectors can be compared with the API's stationDay curve before a single
row is written to the readings tables. Production ingest comes after that
comparison (see the plan in README "Scaling checkpoints").

Design notes
- paho-mqtt runs its network loop in its own thread; messages cross into the
  asyncio world through a queue and are inserted in batches over the direct
  Postgres connection (api/db.py), ~2 s or 500 messages at a time.
- MQTT 5 with a persistent session (clean_start=False, SessionExpiryInterval
  1 h) and QoS 1: messages published while the worker restarts or redeploys
  wait at the broker instead of being lost. The client id is therefore fixed.
- paho reconnects by itself (reconnect_delay_set); a disconnect is a warning,
  not a task failure. If the EMQX_* settings are absent the task says so once
  and exits, so a deploy without them changes nothing.
- The whole message is stored even when it is not the expected shape (then
  under {"_raw": ...}); the test needs to see surprises, not hide them.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import ssl
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from api import db

log = logging.getLogger("solis_forward")

EMQX_HOST = os.getenv("EMQX_HOST", "").strip()
EMQX_PORT = int(os.getenv("EMQX_PORT", "8883"))
EMQX_USER = os.getenv("EMQX_WORKER_USER", "").strip()
EMQX_PASSWORD = os.getenv("EMQX_WORKER_PASSWORD", "")
EMQX_TOPIC = os.getenv("EMQX_TOPIC", "solviva/solis/#")
EMQX_CLIENT_ID = os.getenv("EMQX_CLIENT_ID", "solviva-worker-forward")
SESSION_EXPIRY_SECONDS = int(os.getenv("EMQX_SESSION_EXPIRY_SECONDS", "3600"))
BATCH_ROWS = 500
BATCH_SECONDS = 2.0
LOG_EVERY_SECONDS = 3600


def parse_forward_message(payload: bytes, topic: str) -> Dict[str, Any]:
    """The row to store for one message. Never raises: an unexpected body is
    kept verbatim under payload._raw so the test surfaces it."""
    row: Dict[str, Any] = {
        "device_time": None, "collector_sn": None, "is_realtime": None,
        "inverter_count": None, "topic": topic, "bytes": len(payload), "payload": None,
    }
    try:
        body = json.loads(payload.decode("utf-8"))
    except Exception:
        row["payload"] = {"_raw": payload.decode("utf-8", errors="replace")[:20000]}
        return row
    if not isinstance(body, dict):
        row["payload"] = {"_raw": body}
        return row
    row["payload"] = body
    t = body.get("time")
    try:
        if t is not None and t != "":
            t = float(t)
            if t > 1e11:                      # milliseconds
                t = t / 1000.0
            row["device_time"] = datetime.fromtimestamp(t, tz=timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError):
        pass
    sn = body.get("sn")
    row["collector_sn"] = str(sn).strip() if sn not in (None, "") else None
    rt = body.get("isRealtime")
    if rt in (0, 1, "0", "1", True, False):
        row["is_realtime"] = str(rt) in ("0", "False") if not isinstance(rt, bool) else (not rt)
    inv = body.get("inverter1")
    row["inverter_count"] = len(inv) if isinstance(inv, list) else (1 if isinstance(inv, dict) else None)
    return row


def insert_rows(rows: List[Dict[str, Any]]) -> int:
    """Batch insert over the direct connection. Returns rows written."""
    if not rows:
        return 0
    with db.connect(autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """insert into public.solis_forward_messages
                       (received_at, device_time, collector_sn, is_realtime, inverter_count, topic, bytes, payload)
                   values (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)""",
                [(r["received_at"], r["device_time"], r["collector_sn"], r["is_realtime"], r["inverter_count"],
                  r["topic"], r["bytes"], json.dumps(r["payload"], default=str)) for r in rows])
    return len(rows)


class ForwardCapture:
    """The worker task. `enabled` is False when the broker settings are absent."""

    def __init__(self) -> None:
        self.enabled = bool(EMQX_HOST and EMQX_USER and EMQX_PASSWORD)
        self._q: "queue.Queue[Tuple[float, str, bytes]]" = queue.Queue()
        self.received = 0
        self.stored = 0
        self.failed_batches = 0
        self.connected = False
        self.last_message_at: Optional[float] = None

    # -- paho callbacks (network thread) ------------------------------------
    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        self.connected = reason_code == 0 or getattr(reason_code, "is_failure", lambda: False)() is False
        if self.connected:
            log.info("forward: connected to %s:%d as %s (session present: %s); subscribing %s",
                     EMQX_HOST, EMQX_PORT, EMQX_USER, getattr(flags, "session_present", "?"), EMQX_TOPIC)
            client.subscribe(EMQX_TOPIC, qos=1)
        else:
            log.warning("forward: connect refused: %s", reason_code)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        self.connected = False
        log.warning("forward: disconnected (%s); paho will reconnect", reason_code)

    def _on_message(self, client, userdata, msg) -> None:
        self._q.put((time.time(), msg.topic, bytes(msg.payload)))

    # -- the asyncio side ----------------------------------------------------
    def _drain(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        while len(rows) < BATCH_ROWS:
            try:
                received, topic, payload = self._q.get_nowait()
            except queue.Empty:
                break
            row = parse_forward_message(payload, topic)
            row["received_at"] = datetime.fromtimestamp(received, tz=timezone.utc)
            rows.append(row)
        return rows

    async def run(self, stop: asyncio.Event) -> None:
        if not self.enabled:
            log.info("forward task disabled: set EMQX_HOST, EMQX_WORKER_USER and EMQX_WORKER_PASSWORD to enable it")
            return
        import paho.mqtt.client as mqtt
        from paho.mqtt.enums import CallbackAPIVersion
        from paho.mqtt.packettypes import PacketTypes
        from paho.mqtt.properties import Properties

        client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=EMQX_CLIENT_ID, protocol=mqtt.MQTTv5)
        client.username_pw_set(EMQX_USER, EMQX_PASSWORD)
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        client.reconnect_delay_set(min_delay=2, max_delay=60)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        props = Properties(PacketTypes.CONNECT)
        props.SessionExpiryInterval = SESSION_EXPIRY_SECONDS
        client.connect_async(EMQX_HOST, EMQX_PORT, keepalive=60, clean_start=False, properties=props)
        client.loop_start()
        log.info("forward: capture started (topic %s, client id %s)", EMQX_TOPIC, EMQX_CLIENT_ID)
        last_log = time.time()
        pending: List[Dict[str, Any]] = []
        last_flush = time.time()
        try:
            while not stop.is_set():
                pending.extend(self._drain())
                self.received += 0  # counted at store time below
                if pending and (len(pending) >= BATCH_ROWS or time.time() - last_flush >= BATCH_SECONDS):
                    batch, pending = pending, []
                    try:
                        n = await asyncio.to_thread(insert_rows, batch)
                        self.stored += n
                        self.received += len(batch)
                        self.last_message_at = time.time()
                    except Exception as exc:
                        self.failed_batches += 1
                        log.error("forward: insert of %d message(s) failed (%s); re-queuing", len(batch), str(exc)[:160])
                        pending = batch + pending
                        await asyncio.sleep(5)
                    last_flush = time.time()
                if time.time() - last_log >= LOG_EVERY_SECONDS:
                    last_log = time.time()
                    log.info("forward: %d message(s) stored so far, %d failed batch(es), connected=%s",
                             self.stored, self.failed_batches, self.connected)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass
        finally:
            client.loop_stop()
            try:
                client.disconnect()
            except Exception:
                pass
            if pending:
                try:
                    insert_rows(pending)
                except Exception as exc:
                    log.error("forward: %d message(s) lost at shutdown: %s", len(pending), str(exc)[:120])
            log.info("forward: capture stopped (%d stored)", self.stored)
