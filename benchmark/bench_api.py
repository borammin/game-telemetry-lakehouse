"""Measure p50/p95/p99 latency of the API with and without the cache.

Usage:
  CACHE_ENABLED=0 uvicorn api.main:app --port 8000   # terminal 1
  python benchmark/bench_api.py --label uncached      # terminal 2
  (restart with CACHE_ENABLED=1, run with --label cached)
"""
import argparse
import random
import statistics
import time

import httpx


def pct(values, q):
    values = sorted(values)
    return values[min(len(values) - 1, int(len(values) * q))]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="http://127.0.0.1:8000")
    p.add_argument("--requests", type=int, default=2000)
    p.add_argument("--players", type=int, default=200_000)
    p.add_argument("--hot-set", type=int, default=200, help="players making up 80% of traffic")
    p.add_argument("--label", default="run")
    a = p.parse_args()

    rng = random.Random(1)
    hot = [f"P{rng.randint(1, a.players):07d}" for _ in range(a.hot_set)]

    def pick():
        if rng.random() < 0.8:
            return f"/players/{rng.choice(hot)}/stats"
        return rng.choice(["/stats/genres", "/stats/daily-active"])

    lat = []
    with httpx.Client(base_url=a.base, timeout=30) as c:
        for _ in range(100):  # warmup
            c.get(pick())
        for _ in range(a.requests):
            t = time.perf_counter()
            r = c.get(pick())
            lat.append((time.perf_counter() - t) * 1000)
            assert r.status_code in (200, 404)

    print(f"[{a.label}] n={len(lat)}  mean={statistics.mean(lat):.1f}ms  "
          f"p50={pct(lat, .50):.1f}ms  p95={pct(lat, .95):.1f}ms  p99={pct(lat, .99):.1f}ms")


if __name__ == "__main__":
    main()
