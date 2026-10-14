"""
Shared HTTP auth for the Render-hosted MCP servers (solviva_mcp.py, db_mcp.py).

A fixed service token that claude.ai sends as a request header, plus an
allowlist of Anthropic's egress range so a leaked token is useless from
anywhere else. No OAuth on purpose: see BearerAuthMiddleware.

Lived inside solviva_mcp.py until 2026-10-14; moved here unchanged when
db_mcp.py (the "Solviva Databases" connector) needed the same gate.
"""

from __future__ import annotations

import hmac
import ipaddress
import json
from typing import Any, Dict, Optional

# Anthropic's published egress range. Every request Claude makes to a connector
# comes from here, so anything else is not Claude.
ANTHROPIC_EGRESS = ipaddress.ip_network("160.79.104.0/21")


class BearerAuthMiddleware:
    """Shared service-account token + Anthropic IP allowlist.

    Pure ASGI rather than BaseHTTPMiddleware: streamable HTTP holds long-lived
    SSE responses open, and BaseHTTPMiddleware buffers them.
    """

    def __init__(self, app, token: str, restrict_ips: bool = True) -> None:
        self.app = app
        self.expected = f"Bearer {token}"
        self.restrict_ips = restrict_ips

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Render's health check must not need the token.
        if scope["path"] == "/healthz":
            await _send_json(send, 200, {"status": "ok"})
            return

        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}

        if self.restrict_ips and not _from_anthropic(headers, scope):
            # Log the chain: getting the proxy-hop depth wrong here silently
            # rejects every legitimate request, and the header is the only way
            # to see what the platform actually forwarded.
            print(
                f"[auth] rejected {scope['path']} — resolved client "
                f"{_client_ip(headers, scope)} not in {ANTHROPIC_EGRESS}; "
                f"x-forwarded-for={headers.get('x-forwarded-for', '(none)')!r}",
                flush=True,
            )
            await _send_json(send, 403, {"error": "forbidden"})
            return

        # Constant-time compare so the token can't be recovered by timing.
        #
        # Deliberately no WWW-Authenticate header: that would tell Claude this is
        # an OAuth resource server, sending it looking for discovery documents we
        # do not serve, and the connector would fail with "Couldn't reach the MCP
        # server". This server uses a fixed shared credential, which Claude sends
        # as a configured request header rather than discovering.
        supplied = headers.get("authorization", "")
        if not hmac.compare_digest(supplied, self.expected):
            # Distinguish "the client sent no credential" from "the credential is
            # wrong" — the two have completely different fixes, and from outside
            # both look like an identical 401. Never log the token itself.
            if not supplied:
                detail = "no Authorization header sent"
            elif not supplied.startswith("Bearer "):
                detail = f"Authorization header missing 'Bearer ' prefix (starts {supplied[:8]!r})"
            else:
                detail = (
                    f"token mismatch: got {len(supplied) - 7} chars, "
                    f"expected {len(self.expected) - 7}"
                )
            print(f"[auth] 401 on {scope['path']} — {detail}", flush=True)
            await _send_json(send, 401, {"error": "invalid_token"})
            return

        await self.app(scope, receive, send)


def _client_ip(headers: Dict[str, str], scope) -> Optional[ipaddress._BaseAddress]:
    """The caller's real public IP, as seen through Render's proxy chain.

    Render fronts every service with Cloudflare, which sets CF-Connecting-IP to
    the true client address and overwrites whatever the client sent — so that
    header is both authoritative and unforgeable here, and it is checked first.

    The X-Forwarded-For fallback exists for running behind some other proxy. It
    walks from the right and takes the first PUBLIC address, because leading
    entries are client-supplied and trailing entries are internal hops. Note this
    fallback is exactly what fails on Cloudflare: its edge IPs (172.64.0.0/13 and
    friends) are public, so the walk stops at Cloudflare instead of the caller.
    """
    for header in ("cf-connecting-ip", "true-client-ip"):
        raw = headers.get(header, "").strip()
        if raw:
            try:
                return ipaddress.ip_address(raw)
            except ValueError:
                pass

    chain = [p.strip() for p in headers.get("x-forwarded-for", "").split(",") if p.strip()]
    if not chain:
        chain = [(scope.get("client") or ("",))[0]]

    for raw in reversed(chain):
        try:
            addr = ipaddress.ip_address(raw)
        except ValueError:
            continue
        if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved:
            continue
        return addr
    return None


def _from_anthropic(headers: Dict[str, str], scope) -> bool:
    addr = _client_ip(headers, scope)
    return addr is not None and addr in ANTHROPIC_EGRESS


async def _send_json(send, status: int, body: Dict[str, Any], extra_headers=None) -> None:
    payload = json.dumps(body).encode()
    headers = [(b"content-type", b"application/json"), (b"content-length", str(len(payload)).encode())]
    headers.extend(extra_headers or [])
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": payload})
