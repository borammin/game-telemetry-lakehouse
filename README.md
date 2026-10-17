# Game Telemetry Lakehouse

A small lakehouse for player telemetry, modeled on the kind of player-statistics data behind a game platform. Raw events flow through a **medallion architecture** (bronze, silver, gold) built with PySpark and Delta Lake, and the gold layer is served through a **cached FastAPI** service and a React dashboard.

```
generator ──> raw JSONL ──> bronze (Delta) ──> silver (Delta, partitioned) ──> gold (Delta + Parquet) ──> FastAPI + TTL cache ──> React
```

## Layers

| Layer | What happens |
|-------|--------------|
| Bronze | Raw JSON ingested as-is with an ingest timestamp |
| Silver | Typed timestamps, rows with a null player or non-positive playtime dropped, duplicate `event_id`s removed, partitioned by `event_date` |
| Gold | `player_stats`, `player_top_titles`, `genre_stats`, `daily_active`, also exported to Parquet for serving |

## Run it

```bash
pip install -r requirements.txt

python generator/generate_events.py --rows 5000000 --out data/raw
python pipeline/medallion.py            # prints bronze/silver row counts and timings

uvicorn api.main:app --port 8000        # then open frontend/index.html
pytest -q
```

Or with Docker: `docker compose up --build`

## Benchmark the cache

```bash
CACHE_ENABLED=0 uvicorn api.main:app --port 8000
python benchmark/bench_api.py --label uncached   # stop the server

CACHE_ENABLED=1 uvicorn api.main:app --port 8000
python benchmark/bench_api.py --label cached
```

Results (fill in from your run):

| Setup | Rows processed | p50 | p95 | p99 |
|-------|----------------|-----|-----|-----|
| No cache | [ ] | [ ] | [ ] | [ ] |
| TTL cache | [ ] | [ ] | [ ] | [ ] |

## Design notes

- The API reads Parquet exports of the gold tables so serving doesn't need a Spark runtime.
- The cache is a thread-safe TTL cache with a size cap and hit/miss counters (`/cache/stats`).
- The traffic model sends 80% of requests to a small hot set of players, which is why caching helps. Say this when you quote the numbers.
- CI runs the tests, a small end-to-end pipeline run and a Docker build on every push.
