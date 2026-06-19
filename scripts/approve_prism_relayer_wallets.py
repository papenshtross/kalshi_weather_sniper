#!/usr/bin/env python3
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import requests
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_abi import decode, encode
from eth_utils import function_signature_to_4byte_selector, to_checksum_address
from py_builder_signing_sdk.config import BuilderApiKeyCreds, BuilderConfig

from polybot.security.wallet_registry import decrypt_registry

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "reports" / "deployment" / "prism_relayer_wallet_approvals_2026-06-04.json"
RELAYER = "https://relayer-v2.polymarket.com"
RPC = "https://polygon-bor-rpc.publicnode.com"
CHAIN_ID = 137
FACTORY = "0x00000000000Fb5C9ADea0298D729A0CB3823Cc07"
PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
CTF = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_EXCHANGE = "0xE111180000d2663C0091e4f400237545B87B996B"
NEG_RISK_CTF_EXCHANGE = "0xe2222d279d744050d28e00520010520000310F59"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
MAX_UINT = 2**256 - 1

ERC20_SPENDERS = [CTF_EXCHANGE, NEG_RISK_CTF_EXCHANGE, NEG_RISK_ADAPTER]
ERC1155_OPERATORS = [CTF_EXCHANGE, NEG_RISK_CTF_EXCHANGE, NEG_RISK_ADAPTER]


def builder_headers(cfg: BuilderConfig, method: str, path: str, body: str | None = None) -> dict[str, str]:
    payload = cfg.generate_builder_headers(method, path, body)
    if payload is None:
        raise RuntimeError("could not generate builder headers")
    h = payload.to_dict()
    h["Content-Type"] = "application/json"
    h["User-Agent"] = "polybot-prism-relayer-approve/1.0"
    return h


def relayer_get(path: str, params: dict[str, str] | None = None) -> tuple[int, Any]:
    r = requests.get(f"{RELAYER}{path}", params=params or {}, timeout=30, headers={"User-Agent": "polybot-prism-relayer-approve/1.0"})
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"raw": r.text[:1000]}


def get_nonce(owner: str) -> str:
    status, data = relayer_get("/nonce", {"address": owner, "type": "WALLET"})
    if status != 200 or not isinstance(data, dict) or data.get("nonce") is None:
        raise RuntimeError(f"bad nonce response for {owner}: {status} {data}")
    return str(data["nonce"])


def poll_tx(txid: str, max_polls: int = 80) -> dict[str, Any]:
    last: Any = None
    for _ in range(max_polls):
        status, data = relayer_get("/transaction", {"id": txid})
        last = data
        tx = data[0] if isinstance(data, list) and data else data if isinstance(data, dict) else {}
        state = str(tx.get("state") or "")
        if state in {"STATE_CONFIRMED", "STATE_MINED", "STATE_FAILED", "STATE_CANCELLED"}:
            return {"status": status, "tx": tx}
        time.sleep(2)
    return {"status": "timeout", "tx": last}


def calldata(sig: str, types: list[str], args: list[Any]) -> str:
    return "0x" + function_signature_to_4byte_selector(sig).hex() + encode(types, args).hex()


def approve_calldata(spender: str) -> str:
    return calldata("approve(address,uint256)", ["address", "uint256"], [spender, MAX_UINT])


def set_approval_for_all_calldata(operator: str) -> str:
    return calldata("setApprovalForAll(address,bool)", ["address", "bool"], [operator, True])


def sign_batch(private_key: str, wallet: str, nonce: str, deadline: str, calls: list[dict[str, str]]) -> str:
    pk = private_key if private_key.startswith("0x") else "0x" + private_key
    typed = {
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
                {"name": "verifyingContract", "type": "address"},
            ],
            "Call": [
                {"name": "target", "type": "address"},
                {"name": "value", "type": "uint256"},
                {"name": "data", "type": "bytes"},
            ],
            "Batch": [
                {"name": "wallet", "type": "address"},
                {"name": "nonce", "type": "uint256"},
                {"name": "deadline", "type": "uint256"},
                {"name": "calls", "type": "Call[]"},
            ],
        },
        "primaryType": "Batch",
        "domain": {"name": "DepositWallet", "version": "1", "chainId": CHAIN_ID, "verifyingContract": wallet},
        "message": {
            "wallet": wallet,
            "nonce": int(nonce),
            "deadline": int(deadline),
            "calls": [{"target": c["target"], "value": int(c["value"]), "data": c["data"]} for c in calls],
        },
    }
    msg = encode_typed_data(full_message=typed)
    sig = Account.sign_message(msg, pk).signature.hex()
    return sig if sig.startswith("0x") else "0x" + sig


def rpc_call(to: str, data: str) -> str:
    r = requests.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": "eth_call", "params": [{"to": to, "data": data}, "latest"]}, timeout=30)
    j = r.json()
    if "error" in j:
        raise RuntimeError(j["error"])
    return j.get("result", "0x")


def allowance(owner: str, spender: str) -> int:
    data = calldata("allowance(address,address)", ["address", "address"], [owner, spender])
    out = rpc_call(PUSD, data)
    return int(decode(["uint256"], bytes.fromhex(out[2:]))[0])


def is_approved_for_all(owner: str, operator: str) -> bool:
    data = calldata("isApprovedForAll(address,address)", ["address", "address"], [owner, operator])
    out = rpc_call(CTF, data)
    return bool(decode(["bool"], bytes.fromhex(out[2:]))[0])


def approval_state(wallet: str) -> dict[str, Any]:
    return {
        "erc20_allowances_ok": {sp: allowance(wallet, sp) > 0 for sp in ERC20_SPENDERS},
        "erc1155_approvals_ok": {op: is_approved_for_all(wallet, op) for op in ERC1155_OPERATORS},
    }


def main() -> None:
    registry = decrypt_registry()
    wallets = registry.get("wallets") or {}
    prism5 = wallets["prism5"]
    key = prism5.get("POLYMARKET_BUILDER_API_KEY") or prism5.get("POLY_BUILDER_API_KEY") or prism5.get("BUILDER_API_KEY")
    secret = prism5.get("POLYMARKET_BUILDER_API_SECRET") or prism5.get("POLY_BUILDER_SECRET") or prism5.get("BUILDER_SECRET")
    passphrase = prism5.get("POLYMARKET_BUILDER_API_PASSPHRASE") or prism5.get("POLY_BUILDER_PASSPHRASE") or prism5.get("BUILDER_PASS_PHRASE")
    if not (key and secret and passphrase):
        raise SystemExit("missing builder api credentials")
    cfg = BuilderConfig(local_builder_creds=BuilderApiKeyCreds(key=key, secret=secret, passphrase=passphrase))

    calls = ([{"target": PUSD, "value": "0", "data": approve_calldata(sp)} for sp in ERC20_SPENDERS]
             + [{"target": CTF, "value": "0", "data": set_approval_for_all_calldata(op)} for op in ERC1155_OPERATORS])

    results = []
    for i in range(1, 11):
        name = f"Prism_relayer_{i}"
        vals = wallets[name]
        owner = vals["POLYMARKET_EOA_ADDRESS"]
        wallet = vals["POLYMARKET_DEPOSIT_WALLET"]
        pk = vals["POLYMARKET_PRIVATE_KEY"]
        before = approval_state(wallet)
        rec: dict[str, Any] = {"name": name, "owner": owner, "deposit_wallet": wallet, "before": before}
        all_before = all(before["erc20_allowances_ok"].values()) and all(before["erc1155_approvals_ok"].values())
        if all_before:
            rec["already_approved"] = True
            rec["after"] = before
            results.append(rec)
            continue
        nonce = get_nonce(owner)
        deadline = str(int(time.time()) + 900)
        sig = sign_batch(pk, wallet, nonce, deadline, calls)
        body_obj = {
            "type": "WALLET",
            "from": owner,
            "to": FACTORY,
            "nonce": nonce,
            "signature": sig,
            "depositWalletParams": {"depositWallet": wallet, "deadline": deadline, "calls": calls},
        }
        body = json.dumps(body_obj, separators=(",", ":"))
        headers = builder_headers(cfg, "POST", "/submit", body)
        r = requests.post(f"{RELAYER}/submit", data=body, headers=headers, timeout=30)
        try:
            resp = r.json()
        except Exception:
            resp = {"raw": r.text[:1000]}
        rec["submit_status"] = r.status_code
        rec["submit_response"] = resp
        txid = resp.get("transactionID") if isinstance(resp, dict) else None
        if r.status_code < 300 and txid:
            rec["poll"] = poll_tx(str(txid))
        else:
            rec["error"] = resp
        time.sleep(2)
        rec["after"] = approval_state(wallet)
        results.append(rec)

    payload = {
        "generated_at": int(time.time()),
        "relayer": RELAYER,
        "factory": FACTORY,
        "contracts": {"pUSD": PUSD, "CTF": CTF, "CTF_EXCHANGE": CTF_EXCHANGE, "NEG_RISK_CTF_EXCHANGE": NEG_RISK_CTF_EXCHANGE, "NEG_RISK_ADAPTER": NEG_RISK_ADAPTER},
        "results": results,
    }
    payload["summary"] = {
        "total": len(results),
        "approved_final": sum(1 for r in results if all(r["after"]["erc20_allowances_ok"].values()) and all(r["after"]["erc1155_approvals_ok"].values())),
        "submitted": sum(1 for r in results if r.get("submit_status", 0) and r.get("submit_status", 0) < 300),
        "errors": sum(1 for r in results if r.get("error")),
    }
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload["summary"], indent=2))
    for r in results:
        state = None; txh = None
        if isinstance(r.get("poll"), dict):
            tx = r["poll"].get("tx") or {}
            if isinstance(tx, dict): state = tx.get("state"); txh = tx.get("transactionHash")
        ok = all(r["after"]["erc20_allowances_ok"].values()) and all(r["after"]["erc1155_approvals_ok"].values())
        print(r["name"], "approved", ok, "submit", r.get("submit_status"), "state", state, "tx", txh, "err", r.get("error"))
    print("report", REPORT)


if __name__ == "__main__":
    main()
