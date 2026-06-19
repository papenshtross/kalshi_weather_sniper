from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .wallet_registry import decrypt_registry, WalletSecretError


@dataclass(frozen=True)
class ApiSecret:
    api_id: str
    values: dict[str, str]


def split_windows_credential_payload(username: str, password: str) -> dict[str, str]:
    """Normalize a Windows Credential Manager generic credential.

    For targets such as KALSHI_API, Windows stores the API key id in the
    credential username and the private-key payload in the password/blob.  The
    password may also include checksum metadata; venue-specific parsers should
    interpret that payload further.
    """
    return {
        "username": username,
        "password": password,
    }


def api_secret(api_id: str) -> ApiSecret | None:
    """Load a non-wallet API secret from the encrypted SOPS registry.

    This shares the same DPAPI-protected age identity and SOPS registry used for
    Polymarket wallet secrets. Secrets live under top-level `apis:`.
    """
    registry_env = os.getenv("POLYBOT_WALLET_REGISTRY")
    registry_path = Path(registry_env) if registry_env else None
    # Let decrypt_registry resolve its default path and error semantics.
    try:
        data: dict[str, Any] = decrypt_registry(registry_path)
    except WalletSecretError:
        raise
    apis = data.get("apis") or {}
    vals = apis.get(api_id)
    if not vals:
        return None
    return ApiSecret(api_id=api_id, values={str(k): str(v) for k, v in vals.items() if v is not None})


def kalshi_api_secret() -> ApiSecret | None:
    return api_secret("kalshi")


def apply_api_secret_to_env(api_id: str) -> ApiSecret | None:
    sec = api_secret(api_id)
    if sec is None:
        return None
    for key, value in sec.values.items():
        if key.isupper():
            os.environ.setdefault(key, value)
    return sec
