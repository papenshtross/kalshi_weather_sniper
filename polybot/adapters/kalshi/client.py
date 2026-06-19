from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode, urlparse

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from polybot.security.kalshi_credentials import load_kalshi_credentials


KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


@dataclass(frozen=True)
class KalshiMarket:
    ticker: str
    event_ticker: str
    series_ticker: str
    title: str
    strike_type: str
    floor_strike: float | None
    cap_strike: float | None
    yes_bid: float | None
    yes_ask: float | None
    no_bid: float | None
    no_ask: float | None
    raw: dict[str, Any]

    @property
    def temp_mid_f(self) -> float | None:
        if self.floor_strike is not None and self.cap_strike is not None:
            return (float(self.floor_strike) + float(self.cap_strike)) / 2.0
        if self.floor_strike is not None:
            return float(self.floor_strike)
        if self.cap_strike is not None:
            return float(self.cap_strike)
        return None


def dollars_to_probability(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        val = float(value)
    except Exception:
        return None
    # Kalshi may return cents (integer 0..99) or *_dollars (0..1).
    if val > 1.0:
        val = val / 100.0
    return max(0.0, min(1.0, val))


class KalshiHttpClient:
    def __init__(self, base_url: str = KALSHI_BASE_URL, *, user_agent: str = "kalshi-weather-sniper/0.1") -> None:
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(20.0, connect=5.0), headers={"User-Agent": user_agent})

    async def aclose(self) -> None:
        await self._http.aclose()

    async def get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        path = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}{path}"
        r = await self._http.get(url, params=params or {})
        r.raise_for_status()
        return r.json()

    async def signed_request(self, method: str, path: str, *, params: dict[str, Any] | None = None, body: dict[str, Any] | None = None) -> dict[str, Any]:
        path = path if path.startswith("/") else f"/{path}"
        query = f"?{urlencode({k: v for k, v in (params or {}).items() if v is not None})}" if params else ""
        path_with_query = f"{path}{query}"
        signing_prefix = urlparse(self.base_url).path.rstrip("/")
        signed_path_with_query = f"{signing_prefix}{path_with_query}"
        body_bytes = json.dumps(body or {}, separators=(",", ":")).encode("utf-8") if body is not None else b""
        headers = auth_headers(method, signed_path_with_query, body_bytes)
        url = f"{self.base_url}{path_with_query}"
        r = await self._http.request(method.upper(), url, content=body_bytes if body is not None else None, headers=headers)
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}
        if r.status_code >= 400:
            raise httpx.HTTPStatusError(f"Kalshi {method.upper()} {path} failed: {r.status_code} {data}", request=r.request, response=r)
        return data

    async def list_markets(self, **params: Any) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        limit_pages = int(params.pop("limit_pages", 20))
        for _ in range(limit_pages):
            q = {k: v for k, v in params.items() if v is not None}
            if cursor:
                q["cursor"] = cursor
            data = await self.get_json("markets", q)
            out.extend(data.get("markets") or [])
            cursor = data.get("cursor") or None
            if not cursor:
                break
        return out

    async def orderbook(self, ticker: str) -> dict[str, Any]:
        return await self.get_json(f"markets/{ticker}/orderbook")

    async def balance(self) -> dict[str, Any]:
        return await self.signed_request("GET", "/portfolio/balance")

    async def create_order(
        self,
        *,
        ticker: str,
        side: str,
        count: int | float,
        price: float,
        time_in_force: str = "immediate_or_cancel",
        client_order_id: str | None = None,
        post_only: bool = False,
        reduce_only: bool = False,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "ticker": ticker,
            "side": side.lower(),
            "count": f"{float(count):.2f}",
            "price": f"{float(price):.4f}",
            "time_in_force": time_in_force,
            "self_trade_prevention_type": "taker_at_cross",
            "post_only": bool(post_only),
            "cancel_order_on_pause": False,
            "reduce_only": bool(reduce_only),
            "subaccount": 0,
            "exchange_index": 0,
        }
        if client_order_id:
            body["client_order_id"] = client_order_id
        return await self.signed_request("POST", "/portfolio/events/orders", body=body)


def parse_market(raw: dict[str, Any]) -> KalshiMarket:
    return KalshiMarket(
        ticker=str(raw.get("ticker") or ""),
        event_ticker=str(raw.get("event_ticker") or ""),
        series_ticker=str(raw.get("series_ticker") or ""),
        title=str(raw.get("title") or ""),
        strike_type=str(raw.get("strike_type") or ""),
        floor_strike=_float_or_none(raw.get("floor_strike")),
        cap_strike=_float_or_none(raw.get("cap_strike")),
        yes_bid=dollars_to_probability(raw.get("yes_bid_dollars", raw.get("yes_bid"))),
        yes_ask=dollars_to_probability(raw.get("yes_ask_dollars", raw.get("yes_ask"))),
        no_bid=dollars_to_probability(raw.get("no_bid_dollars", raw.get("no_bid"))),
        no_ask=dollars_to_probability(raw.get("no_ask_dollars", raw.get("no_ask"))),
        raw=raw,
    )


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def auth_headers(method: str, path_with_query: str, body: bytes = b"") -> dict[str, str]:
    """Build Kalshi RSA-PSS auth headers from the local encrypted registry."""
    creds = load_kalshi_credentials()
    if creds is None:
        raise RuntimeError("Kalshi credentials not found in local registry")
    timestamp_ms = str(int(time.time() * 1000))
    # Kalshi V2 signs timestamp + method + path, with query string excluded.
    parsed = urlparse(path_with_query)
    path_only = parsed.path or path_with_query.split("?", 1)[0]
    msg = f"{timestamp_ms}{method.upper()}{path_only}".encode()
    key = serialization.load_pem_private_key(creds.private_key_pem.encode(), password=None)
    if not isinstance(key, rsa.RSAPrivateKey):
        raise TypeError("Kalshi private key is not RSA")
    sig = key.sign(
        msg,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": creds.key_id,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        "Content-Type": "application/json",
    }
