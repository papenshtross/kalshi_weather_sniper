#!/usr/bin/env python3
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import requests
from py_builder_signing_sdk.config import BuilderApiKeyCreds, BuilderConfig

from polybot.security.wallet_registry import decrypt_registry

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "deployment" / "prism_relayer_wallet_deploy_2026-06-04.json"
RELAYER = "https://relayer-v2.polymarket.com"
FACTORY = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07"
RPC = "https://polygon-bor-rpc.publicnode.com"


def builder_headers(cfg: BuilderConfig, method: str, path: str, body: str | None = None) -> dict[str, str]:
    payload = cfg.generate_builder_headers(method, path, body)
    if payload is None:
        raise RuntimeError("could not generate builder headers")
    h = payload.to_dict()
    h["Content-Type"] = "application/json"
    h["User-Agent"] = "polybot-prism-relayer-deploy/1.0"
    return h


def relayer_get(path: str, params: dict[str, str] | None = None) -> Any:
    r = requests.get(f"{RELAYER}{path}", params=params or {}, timeout=30, headers={"User-Agent": "polybot-prism-relayer-deploy/1.0"})
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"raw": r.text[:1000]}


def deployed(wallet: str) -> bool:
    status, data = relayer_get("/deployed", {"address": wallet, "type": "WALLET"})
    if status == 200 and isinstance(data, dict):
        return bool(data.get("deployed"))
    return False


def code_exists(addr: str) -> bool:
    try:
        r = requests.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": "eth_getCode", "params": [addr, "latest"]}, timeout=30)
        data = r.json()
        code = str(data.get("result") or "")
        return code not in {"", "0x", "0X"}
    except Exception:
        return False


def poll_tx(txid: str, max_polls: int = 60) -> dict[str, Any]:
    last: Any = None
    for _ in range(max_polls):
        status, data = relayer_get("/transaction", {"id": txid})
        last = data
        # API may return array or object depending on relayer version.
        tx = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else {}
        state = str(tx.get("state") or "")
        if state in {"STATE_CONFIRMED", "STATE_MINED", "STATE_FAILED", "STATE_CANCELLED"}:
            return {"status": status, "tx": tx}
        time.sleep(2)
    return {"status": "timeout", "tx": last}


def main() -> None:
    registry = decrypt_registry()
    wallets = registry.get("wallets") or {}
    prism5 = wallets["prism5"]

    # Accept either naming convention; do not print these values.
    key = prism5.get("POLYMARKET_BUILDER_API_KEY") or prism5.get("POLY_BUILDER_API_KEY") or prism5.get("BUILDER_API_KEY")
    secret = prism5.get("POLYMARKET_BUILDER_API_SECRET") or prism5.get("POLY_BUILDER_SECRET") or prism5.get("BUILDER_SECRET")
    passphrase = prism5.get("POLYMARKET_BUILDER_API_PASSPHRASE") or prism5.get("POLY_BUILDER_PASSPHRASE") or prism5.get("BUILDER_PASS_PHRASE")
    if not (key and secret and passphrase):
        raise SystemExit("missing builder api credentials in prism5 registry entry")

    cfg = BuilderConfig(local_builder_creds=BuilderApiKeyCreds(key=key, secret=secret, passphrase=passphrase))

    results = []
    for i in range(1, 11):
        name = f"Prism_relayer_{i}"
        vals = wallets[name]
        owner = vals["POLYMARKET_EOA_ADDRESS"]
        wallet = vals["POLYMARKET_DEPOSIT_WALLET"]
        before_rel = deployed(wallet)
        before_code = code_exists(wallet)
        rec: dict[str, Any] = {
            "name": name,
            "owner": owner,
            "deposit_wallet": wallet,
            "before_relayer_deployed": before_rel,
            "before_code_exists": before_code,
        }
        if before_rel or before_code:
            rec.update({"submitted": False, "already_deployed": True})
            results.append(rec)
            continue

        body_obj = {"type": "WALLET-CREATE", "from": owner, "to": FACTORY}
        body = json.dumps(body_obj, separators=(",", ":"))
        headers = builder_headers(cfg, "POST", "/submit", body)
        try:
            r = requests.post(f"{RELAYER}/submit", data=body, headers=headers, timeout=30)
            try:
                resp = r.json()
            except Exception:
                resp = {"raw": r.text[:1000]}
            rec["submit_status"] = r.status_code
            rec["submit_response"] = resp
            txid = resp.get("transactionID") if isinstance(resp, dict) else None
            if r.status_code < 300 and txid:
                polled = poll_tx(str(txid))
                rec["poll"] = polled
            else:
                rec["error"] = resp
        except Exception as e:
            rec["exception"] = repr(e)
        rec["after_relayer_deployed"] = deployed(wallet)
        rec["after_code_exists"] = code_exists(wallet)
        results.append(rec)

    # Final verification pass after all submissions.
    time.sleep(2)
    for rec in results:
        rec["final_relayer_deployed"] = deployed(rec["deposit_wallet"])
        rec["final_code_exists"] = code_exists(rec["deposit_wallet"])

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": int(time.time()),
        "relayer": RELAYER,
        "factory": FACTORY,
        "builder_key_present": True,
        "results": results,
        "summary": {
            "total": len(results),
            "deployed_final": sum(1 for r in results if r.get("final_relayer_deployed") or r.get("final_code_exists")),
            "submitted": sum(1 for r in results if r.get("submit_status", 0) and r.get("submit_status", 0) < 300),
            "errors": sum(1 for r in results if r.get("error") or r.get("exception")),
        },
    }
    REPORT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2))
    for r in results:
        state = None
        txh = None
        if isinstance(r.get("poll"), dict):
            tx = r["poll"].get("tx") or {}
            if isinstance(tx, dict):
                state = tx.get("state")
                txh = tx.get("transactionHash")
        print(r["name"], "deployed", r.get("final_relayer_deployed"), "code", r.get("final_code_exists"), "submit", r.get("submit_status"), "state", state, "tx", txh, "err", r.get("error") or r.get("exception"))
    print("report", REPORT)


if __name__ == "__main__":
    main()
