from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
import yaml

from polybot.copytrade.ranking import (
    ProfileSnapshot,
    build_watchlist,
    extract_leaderboard_rows,
    extract_next_data,
    extract_profile_snapshot,
)

DEFAULT_ROUTES = [
    ("overall_all_profit", "https://polymarket.com/leaderboard/overall/all/profit"),
    ("overall_all_volume", "https://polymarket.com/leaderboard/overall/all/volume"),
    ("overall_monthly_profit", "https://polymarket.com/leaderboard/overall/monthly/profit"),
    ("overall_weekly_profit", "https://polymarket.com/leaderboard/overall/weekly/profit"),
    ("crypto_all_profit", "https://polymarket.com/leaderboard/crypto/all/profit"),
    ("crypto_all_volume", "https://polymarket.com/leaderboard/crypto/all/volume"),
    ("sports_all_profit", "https://polymarket.com/leaderboard/sports/all/profit"),
    ("sports_all_volume", "https://polymarket.com/leaderboard/sports/all/volume"),
    ("politics_all_profit", "https://polymarket.com/leaderboard/politics/all/profit"),
    ("finance_all_profit", "https://polymarket.com/leaderboard/finance/all/profit"),
    ("tech_all_profit", "https://polymarket.com/leaderboard/tech/all/profit"),
    ("economy_all_profit", "https://polymarket.com/leaderboard/economy/all/profit"),
]


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Hermes polybot copytrade ranker)"})
    return session


def fetch_next_data(session: requests.Session, url: str) -> dict:
    response = session.get(url, timeout=20)
    response.raise_for_status()
    return extract_next_data(response.text)


def collect_candidate_wallets(routes: list[tuple[str, str]]) -> dict[str, dict]:
    session = _session()
    candidates: dict[str, dict] = {}
    for route_name, url in routes:
        next_data = fetch_next_data(session, url)
        for row in extract_leaderboard_rows(next_data):
            address = str(row.get("proxyWallet") or "").lower()
            if not address.startswith("0x") or len(address) != 42:
                continue
            candidate = candidates.setdefault(
                address,
                {
                    "address": address,
                    "name": row.get("name") or row.get("pseudonym") or address,
                    "leaderboard_pnl": float(row.get("pnl") or 0.0),
                    "leaderboard_volume": float(row.get("volume") or 0.0),
                    "routes": [],
                },
            )
            candidate["leaderboard_pnl"] = max(candidate["leaderboard_pnl"], float(row.get("pnl") or 0.0))
            candidate["leaderboard_volume"] = max(candidate["leaderboard_volume"], float(row.get("volume") or 0.0))
            if route_name not in candidate["routes"]:
                candidate["routes"].append(route_name)
    return candidates


def fetch_profile_snapshot(address: str) -> ProfileSnapshot:
    session = _session()
    next_data = fetch_next_data(session, f"https://polymarket.com/profile/{address}")
    return extract_profile_snapshot(next_data, address)


def generate_watchlist(top_n: int, candidate_limit: int, max_workers: int) -> tuple[list[dict], dict[str, dict]]:
    candidates = collect_candidate_wallets(DEFAULT_ROUTES)
    ordered_candidates = sorted(
        candidates.values(),
        key=lambda row: (-len(row["routes"]), -row["leaderboard_pnl"], -row["leaderboard_volume"]),
    )[:candidate_limit]

    snapshots: list[ProfileSnapshot] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fetch_profile_snapshot, row["address"]): row["address"] for row in ordered_candidates}
        for future in as_completed(futures):
            address = futures[future]
            try:
                snapshots.append(future.result())
            except Exception as exc:
                print(f"warning: failed to fetch {address}: {exc}")

    watchlist = build_watchlist(snapshots, top_n=top_n)
    return watchlist, candidates


def write_outputs(watchlist: list[dict], candidates: dict[str, dict], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = int(time.time())
    payload = {
        "generated_at": generated_at,
        "candidate_count": len(candidates),
        "leaderboard_routes": [route_name for route_name, _ in DEFAULT_ROUTES],
        "wallets": watchlist,
    }
    json_path = output_dir / "copytrade_top50_wallets.json"
    yaml_path = output_dir / "copytrade_watchlist.yaml"
    json_path.write_text(json.dumps(payload, indent=2))
    yaml_path.write_text(
        yaml.safe_dump(
            {
                "generated_at": generated_at,
                "mode": "wallet-profile-ranking",
                "wallets": watchlist,
            },
            sort_keys=False,
            allow_unicode=True,
        )
    )
    print(f"wrote {json_path}")
    print(f"wrote {yaml_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rank Polymarket wallets for copy-trading watchlists")
    parser.add_argument("--top", type=int, default=50)
    parser.add_argument("--candidate-limit", type=int, default=120)
    parser.add_argument("--max-workers", type=int, default=12)
    parser.add_argument("--output-dir", type=Path, default=Path("data/copytrade"))
    args = parser.parse_args()

    watchlist, candidates = generate_watchlist(
        top_n=args.top,
        candidate_limit=args.candidate_limit,
        max_workers=args.max_workers,
    )
    write_outputs(watchlist, candidates, args.output_dir)


if __name__ == "__main__":
    main()
