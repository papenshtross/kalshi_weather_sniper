from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Mapping

from cryptography.hazmat.primitives import serialization

from .api_registry import ApiSecret, kalshi_api_secret


_SHA_RE = re.compile(r"(?i)\b(?:sha(?:256)?\s*[:=]?\s*)?([a-f0-9]{64})\b")
_PEM_RE = re.compile(
    r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----",
    re.DOTALL,
)


@dataclass(frozen=True)
class KalshiCredentials:
    key_id: str
    private_key_pem: str
    password_sha256: str | None = None
    source: str | None = None


def _unescape_newlines(value: str) -> str:
    value = value.strip().strip('"').strip("'")
    # Windows Credential Manager / JSON exports often preserve PEMs as literal \n.
    # Some double-escaped JSON paths decode to backslash + actual newline.
    value = value.replace("\\\r\n", "\n").replace("\\\n", "\n")
    # Repeatedly collapse literal backslash-n sequences; double-escaped JSON
    # can require more than one pass.
    while "\\n" in value:
        value = value.replace("\\r\\n", "\n").replace("\\n", "\n")
    return value.replace("\r\n", "\n").replace("\r", "\n")


def _extract_pem_and_sha(password: str) -> tuple[str, str | None]:
    """Parse a Kalshi private-key password field that may include SHA metadata.

    Expected Windows Credential Manager layout:
    - username: Kalshi API key id
    - password: PEM private key, optionally followed/prepended by a SHA/SHA256
      checksum or JSON wrapper.

    Accepted password shapes include:
    - raw PEM
    - raw PEM + trailing SHA line/comment
    - JSON containing private_key/privateKey/pem and sha/sha256 fields
    - escaped-newline PEM text
    """
    text = _unescape_newlines(password)
    sha: str | None = None

    if text.lstrip().startswith("{"):
        try:
            data = json.loads(text)
            for key in ("sha256", "sha", "fingerprint", "private_key_sha256"):
                val = data.get(key)
                if isinstance(val, str):
                    m = _SHA_RE.search(val)
                    if m:
                        sha = m.group(1).lower()
                        break
            for key in ("private_key", "privateKey", "pem", "key", "KALSHI_PRIVATE_KEY"):
                val = data.get(key)
                if isinstance(val, str) and val.strip():
                    text = _unescape_newlines(val)
                    break
        except json.JSONDecodeError:
            pass

    if sha is None:
        m = _SHA_RE.search(text)
        if m:
            sha = m.group(1).lower()

    m = _PEM_RE.search(text)
    if m:
        pem = m.group(0).strip()
    else:
        # No full END marker means this is not a usable PEM. Return the cleaned
        # text so callers can report length/shape without losing information.
        pem = text.strip()

    return pem, sha


def parse_kalshi_credentials(values: Mapping[str, str] | ApiSecret) -> KalshiCredentials:
    raw = values.values if isinstance(values, ApiSecret) else values
    key_id = (
        raw.get("KALSHI_KEY_ID")
        or raw.get("key_id")
        or raw.get("username")
        or raw.get("api_key")
        or raw.get("API_KEY")
        or ""
    ).strip()
    password = (
        raw.get("KALSHI_PRIVATE_KEY")
        or raw.get("private_key")
        or raw.get("password")
        or raw.get("PRIVATE_KEY")
        or ""
    )
    pem, sha = _extract_pem_and_sha(password)
    sha = sha or raw.get("password_sha256") or raw.get("password_sha256_prefix")
    return KalshiCredentials(
        key_id=key_id,
        private_key_pem=pem,
        password_sha256=sha,
        source=raw.get("source"),
    )


def load_kalshi_credentials() -> KalshiCredentials | None:
    sec = kalshi_api_secret()
    if sec is None:
        return None
    return parse_kalshi_credentials(sec)


def validate_kalshi_private_key_pem(private_key_pem: str) -> tuple[bool, str]:
    try:
        serialization.load_pem_private_key(private_key_pem.encode("utf-8"), password=None)
        return True, "ok"
    except Exception as exc:  # noqa: BLE001 - validation helper returns class name
        return False, type(exc).__name__
