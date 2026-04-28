"""Same perf scenarios as perf_test.py, but for Lab 3 (synchronous POST, no MQ)."""
import argparse
import concurrent.futures
import json
import time

import requests

FACADE = "http://localhost:8082"


def reset_stats():
    requests.post(f"{FACADE}/stats/reset")


def get_stats():
    return requests.get(f"{FACADE}/stats").json()


def worker(user_id, count):
    s = requests.Session()
    for _ in range(count):
        s.post(f"{FACADE}/transaction", json={"user_id": user_id, "amount": 1})


def run_scenario(name, user_ids, count_per_client):
    print(f"--- {name}: {len(user_ids)} clients × {count_per_client} req ---", flush=True)
    reset_stats()
    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(user_ids)) as pool:
        futures = [pool.submit(worker, uid, count_per_client) for uid in user_ids]
        concurrent.futures.wait(futures)
    elapsed = time.time() - start
    stats = get_stats()
    res = {
        "scenario": name,
        "clients": len(user_ids),
        "requests_per_client": count_per_client,
        "total_requests": len(user_ids) * count_per_client,
        "total_time_s": round(elapsed, 3),
        "logging_service_total_s": round(stats["logging_service_total_seconds"], 3),
        "counter_service_total_s": round(stats["counter_service_total_seconds"], 3),
    }
    print(json.dumps(res, indent=2), flush=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", type=int, default=10)
    ap.add_argument("--per-client", type=int, default=200)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    results = []
    results.append(run_scenario(
        "10 accounts",
        [f"perf_user_{i}" for i in range(args.clients)],
        args.per_client,
    ))
    results.append(run_scenario(
        "1 account",
        ["perf_shared"] * args.clients,
        args.per_client,
    ))

    if args.out:
        with open(args.out, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Wrote {args.out}")
