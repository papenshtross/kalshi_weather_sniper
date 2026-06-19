from __future__ import annotations

import base64
import time
from dataclasses import dataclass
from typing import Any

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
        url = f"{self.base_url}/{path.lstrip('/')}"
        r = await self._http.get(url, params=params or {})
        r.raise_for_status()
        return r.json()

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
    """Build Kalshi RSA-PSS auth headers from the local encrypted registry.

    This is intentionally small and isolated; the runner defaults to dry-run and
    does not submit orders unless explicitly enabled.
    """
    creds = load_kalshi_credentials()
    if creds is None:
        raise RuntimeError("Kalshi credentials not found in local registry")
    timestamp_ms = str(int(time.time() * 1000))
    msg = f"{timestamp_ms}{method.upper()}{path_with_query}".encode() + body
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
