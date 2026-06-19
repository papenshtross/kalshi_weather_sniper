"""Thin wrapper around py-clob-client-v2.

Handles:
- auth setup (API key creation / derivation via L1 signature)
- common request retry + rate limit backoff
- shared httpx client for REST calls Goldsky/Gamma that py-clob-client-v2 doesn't expose

This is intentionally framework-agnostic — it knows nothing about Nautilus.
The Nautilus DataClient / ExecutionClient wrap this.
"""
from __future__ import annotations

import asyncio
import os
import threading
from dataclasses import dataclass
from typing import Any

import httpx
from loguru import logger

try:
    from py_clob_client_v2.client import ClobClient
    from py_clob_client_v2.clob_types import ApiCreds, BuilderConfig
except ImportError:  # pragma: no cover — stub environment
    ClobClient = None  # type: ignore
    ApiCreds = None  # type: ignore
    BuilderConfig = None  # type: ignore


def _config_from_wallet_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "private_key": row.get("private_key_encrypted") or row.get("private_key"),
        "proxy_address": row.get("proxy_address"),
        "signature_type": int(row.get("signature_type") or 1),
        "api_key": row.get("clob_api_key") or row.get("api_key"),
        "api_secret": row.get("clob_api_secret") or row.get("api_secret"),
        "api_passphrase": row.get("clob_api_passphrase") or row.get("api_passphrase"),
    }


def _load_wallet_config_from_db(db_url: str | None) -> dict[str, Any]:
    """Load dashboard wallet_config synchronously, including inside a running loop."""
    if not db_url:
        return {}
    try:
        import asyncpg
    except ImportError:
        return {}

    async def _query() -> dict[str, Any]:
        conn = await asyncpg.connect(db_url)
        try:
            row = await conn.fetchrow(
                """
                SELECT private_key_encrypted, private_key, proxy_address, signature_type,
                       clob_api_key, clob_api_secret, clob_api_passphrase
                FROM wallet_config
                WHERE id = 'default'
                """
            )
            return _config_from_wallet_row(dict(row)) if row else {}
        finally:
            await conn.close()

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_query())

    result: dict[str, Any] = {}
    error: BaseException | None = None

    def _runner() -> None:
        nonlocal result, error
        try:
            result = asyncio.run(_query())
        except BaseException as exc:  # pragma: no cover - defensive path
            error = exc

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    t.join(timeout=10)
    if t.is_alive():
        logger.warning("Timed out loading Polymarket wallet_config from DB")
        return {}
    if error:
        logger.warning("Failed to load Polymarket wallet_config from DB: {}", error)
        return {}
    return result


@dataclass
class PolymarketConfig:
    host: str = "https://clob.polymarket.com"
    chain_id: int = 137
    private_key: str | None = None
    proxy_address: str | None = None
    signature_type: int = 1  # POLY_PROXY/Magic-link profile default; override env for Gnosis Safe
    api_key: str | None = None
    api_secret: str | None = None
    api_passphrase: str | None = None
    builder_code: str | None = None
    builder_address: str | None = None
    retry_on_error: bool = True

    @classmethod
    def from_env(cls) -> "PolymarketConfig":
        wallet_values: dict[str, str] = {}
        try:
            from polybot.security.wallet_registry import wallet_secret

            secret = wallet_secret(proxy_address=os.getenv("POLYMARKET_PROXY_ADDRESS") or None)
            wallet_values = secret.values if secret else {}
            if secret:
                logger.info("Polymarket wallet secrets loaded from encrypted registry wallet_id={}", secret.wallet_id)
        except Exception as exc:
            if os.getenv("POLYBOT_WALLET_ID") or os.getenv("POLYMARKET_WALLET_ID") or os.getenv("POLYBOT_WALLET_REGISTRY"):
                raise
            logger.debug("No encrypted Polymarket wallet registry loaded: {}", exc)

        def val(name: str, *alts: str) -> str | None:
            for key in (name, *alts):
                v = os.getenv(key)
                if v:
                    return v
                v = wallet_values.get(key)
                if v:
                    return v
            return None

        cfg = cls(
            host=val("POLYMARKET_HOST") or "https://clob.polymarket.com",
            chain_id=int(val("POLYMARKET_CHAIN_ID") or "137"),
            private_key=val("POLYMARKET_PRIVATE_KEY"),
            proxy_address=val("POLYMARKET_PROXY_ADDRESS") or None,
            signature_type=int(val("POLYMARKET_SIGNATURE_TYPE") or "1"),
            api_key=val("POLYMARKET_API_KEY", "POLY_API_KEY"),
            api_secret=val("POLYMARKET_API_SECRET", "POLY_API_SECRET"),
            api_passphrase=val("POLYMARKET_API_PASSPHRASE", "POLY_PASSPHRASE"),
            builder_code=val("POLYMARKET_BUILDER_CODE", "POLY_BUILDER_CODE"),
            builder_address=val("POLYMARKET_BUILDER_ADDRESS", "POLY_BUILDER_ADDRESS"),
            retry_on_error=((val("POLYMARKET_RETRY_ON_ERROR") or "true").strip().lower() not in {"0", "false", "no", "off"}),
        )
        if not cfg.private_key:
            row = _load_wallet_config_from_db(
                os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL") or os.getenv("NAUTILUS_DB_URL")
            )
            if row:
                cfg.private_key = row.get("private_key") or cfg.private_key
                cfg.proxy_address = row.get("proxy_address") or cfg.proxy_address
                cfg.signature_type = int(row.get("signature_type") or cfg.signature_type)
                cfg.api_key = row.get("api_key") or cfg.api_key
                cfg.api_secret = row.get("api_secret") or cfg.api_secret
                cfg.api_passphrase = row.get("api_passphrase") or cfg.api_passphrase
                logger.info("Polymarket CLOB config loaded from dashboard wallet_config")
        return cfg


class PolymarketHttpClient:
    """Unified Polymarket access: CLOB (via py-clob-client-v2) + Gamma REST."""

    def __init__(self, cfg: PolymarketConfig | None = None) -> None:
        self.cfg = cfg or PolymarketConfig.from_env()
        self._clob: Any = None
        self._http = httpx.AsyncClient(timeout=httpx.Timeout(10.0))

    # ------------------------------------------------------------------ CLOB

    def _build_clob(self) -> Any:
        if ClobClient is None:
            raise RuntimeError("py-clob-client-v2 not installed")
        if not self.cfg.private_key:
            raise RuntimeError("POLYMARKET_PRIVATE_KEY not set")
        builder_config = None
        if (self.cfg.builder_code or self.cfg.builder_address) and BuilderConfig is not None:
            builder_config = BuilderConfig(
                builder_address=self.cfg.builder_address or "",
                builder_code=self.cfg.builder_code or "0x" + "0" * 64,
            )
        client = ClobClient(
            host=self.cfg.host,
            key=self.cfg.private_key,
            chain_id=self.cfg.chain_id,
            signature_type=self.cfg.signature_type,
            funder=self.cfg.proxy_address,
            builder_config=builder_config,
            retry_on_error=self.cfg.retry_on_error,
        )
        if self.cfg.api_key and self.cfg.api_secret and self.cfg.api_passphrase and ApiCreds is not None:
            creds = ApiCreds(
                api_key=self.cfg.api_key,
                api_secret=self.cfg.api_secret,
                api_passphrase=self.cfg.api_passphrase,
            )
            client.set_api_creds(creds)
            logger.info("Polymarket CLOB client initialised using provided dashboard L2 creds")
        else:
            creds = client.create_or_derive_api_key()
            client.set_api_creds(creds)
            logger.info("Polymarket CLOB client initialised (L2 auth derived)")
        return client

    @property
    def clob(self) -> Any:
        if self._clob is None:
            self._clob = self._build_clob()
        return self._clob

    # ------------------------------------------------------------------ Gamma

    async def gamma_markets(self, **params: Any) -> list[dict[str, Any]]:
        """Fetch markets metadata from the Gamma REST API."""
        r = await self._http.get(
            "https://gamma-api.polymarket.com/markets", params=params
        )
        r.raise_for_status()
        return r.json()

    async def aclose(self) -> None:
        await self._http.aclose()
