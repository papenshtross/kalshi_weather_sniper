from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_REGISTRY = ROOT / "config" / "wallets" / "wallets.sops.yaml"
DEFAULT_DPAPI_BLOB_WIN = r"C:\Users\Administrator\AppData\Local\Polybot\age-identity.dpapi"
RETRIEVE_PS1 = Path(os.getenv("POLYBOT_DPAPI_AGE_RETRIEVE_PS1", str(ROOT / "scripts" / "secrets" / "dpapi_age_identity_retrieve.ps1")))
FALLBACK_RETRIEVE_PS1 = Path("/home/administrator/projects/polybot/scripts/secrets/dpapi_age_identity_retrieve.ps1")
POWERSHELL = "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe"
SOPS = os.getenv("SOPS_BIN", "/home/administrator/.local/bin/sops")


class WalletSecretError(RuntimeError):
    pass


@dataclass(frozen=True)
class WalletSecret:
    wallet_id: str
    values: dict[str, str]


def _wslpath_win(path: Path) -> str:
    """Return a Windows path usable by PowerShell launched from WSL.

    PowerShell on this host cannot execute scripts through the \\wsl.localhost UNC
    path that `wslpath -w` returns for Linux-home files. Copy short helper
    scripts to the Windows temp directory and execute them from C:\\ instead.
    """
    win = subprocess.check_output(["wslpath", "-w", str(path)], text=True).strip()
    if win.startswith(r"\\wsl.localhost") and path.suffix.lower() == ".ps1":
        tmp = Path("/mnt/c/Users/Administrator/AppData/Local/Temp") / path.name
        tmp.write_bytes(path.read_bytes())
        return str(subprocess.check_output(["wslpath", "-w", str(tmp)], text=True).strip())
    return win


def dpapi_age_identity(blob_win: str | None = None) -> str:
    """Return the DPAPI-protected age identity, without writing it to disk."""
    blob = blob_win or os.getenv("POLYBOT_SOPS_AGE_DPAPI_BLOB") or DEFAULT_DPAPI_BLOB_WIN
    retrieve_ps1 = RETRIEVE_PS1 if RETRIEVE_PS1.exists() else FALLBACK_RETRIEVE_PS1
    if not retrieve_ps1.exists():
        raise WalletSecretError(f"DPAPI retrieve script missing: {RETRIEVE_PS1}")
    try:
        out = subprocess.check_output(
            [
                "./WindowsPowerShell/v1.0/powershell.exe",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                _wslpath_win(retrieve_ps1),
                "-InFile",
                blob,
            ],
            text=True,
            stderr=subprocess.PIPE,
            cwd="/mnt/c/Windows/System32",
        )
    except subprocess.CalledProcessError as exc:
        raise WalletSecretError(f"failed to retrieve DPAPI age identity: {exc.stderr.strip() or exc}") from exc
    ident = out.replace("\r", "").strip()
    if not ident.startswith("AGE-SECRET-KEY-"):
        raise WalletSecretError("DPAPI output did not look like an age identity")
    return ident


def decrypt_registry(registry: Path | None = None) -> dict[str, Any]:
    """Decrypt the SOPS wallet registry into memory."""
    registry_path = Path(os.getenv("POLYBOT_WALLET_REGISTRY", str(registry or DEFAULT_REGISTRY)))
    if not registry_path.exists():
        raise WalletSecretError(f"wallet registry not found: {registry_path}")
    age_key = dpapi_age_identity()
    env = dict(os.environ)
    env["SOPS_AGE_KEY"] = age_key
    # The global Hermes shell may define SOPS_CONFIG/SOPS_AGE_KEY_FILE for a
    # different secrets store. For this registry, the DPAPI age key is the
    # authoritative identity, so prevent unrelated config from intercepting.
    env.pop("SOPS_CONFIG", None)
    env.pop("SOPS_AGE_KEY_FILE", None)
    try:
        raw = subprocess.check_output(
            [SOPS, "--decrypt", "--output-type", "json", str(registry_path)],
            text=True,
            stderr=subprocess.PIPE,
            env=env,
        )
    except subprocess.CalledProcessError as exc:
        raise WalletSecretError(f"failed to decrypt wallet registry: {exc.stderr.strip() or exc}") from exc
    finally:
        # Best effort: do not keep a live reference around longer than needed.
        age_key = ""
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise WalletSecretError("decrypted wallet registry was not valid JSON") from exc


def wallet_secret(wallet_id: str | None = None, *, proxy_address: str | None = None) -> WalletSecret | None:
    """Load a wallet by explicit id, or match by proxy/funder address.

    Returns None when no encrypted registry is configured/present; raises on decrypt
    or lookup errors when a wallet was explicitly requested.
    """
    requested = wallet_id or os.getenv("POLYBOT_WALLET_ID") or os.getenv("POLYMARKET_WALLET_ID")
    registry_path = Path(os.getenv("POLYBOT_WALLET_REGISTRY", str(DEFAULT_REGISTRY)))
    if not registry_path.exists():
        if requested:
            raise WalletSecretError(f"wallet registry missing for requested wallet {requested!r}: {registry_path}")
        return None
    data = decrypt_registry(registry_path)
    wallets = data.get("wallets") or {}
    if requested:
        if requested not in wallets:
            raise WalletSecretError(f"wallet id {requested!r} not found in encrypted registry")
        vals = {str(k): str(v) for k, v in (wallets[requested] or {}).items() if v is not None}
        return WalletSecret(requested, vals)
    proxy = (proxy_address or os.getenv("POLYMARKET_PROXY_ADDRESS") or "").lower()
    if proxy:
        for wid, vals0 in wallets.items():
            vals = vals0 or {}
            candidates = [
                vals.get("POLYMARKET_PROXY_ADDRESS"),
                vals.get("proxy_address"),
                vals.get("deposit_wallet"),
                vals.get("funder"),
            ]
            if any(str(c or "").lower() == proxy for c in candidates):
                return WalletSecret(str(wid), {str(k): str(v) for k, v in vals.items() if v is not None})
    if len(wallets) == 1:
        wid, vals0 = next(iter(wallets.items()))
        return WalletSecret(str(wid), {str(k): str(v) for k, v in (vals0 or {}).items() if v is not None})
    return None


def apply_wallet_secret_to_env(wallet_id: str | None = None) -> WalletSecret | None:
    """Populate missing POLYMARKET_* env vars from the encrypted registry.

    Existing environment values win so emergency overrides remain possible.
    """
    sec = wallet_secret(wallet_id)
    if sec is None:
        return None
    for key, value in sec.values.items():
        if key.startswith("POLYMARKET_") or key.startswith("POLY_") or key in {"PROXY_ADDRESS"}:
            os.environ.setdefault(key, value)
    return sec
