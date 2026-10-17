"""FastAPI service over the gold layer (Parquet export), with an optional TTL cache.

Set CACHE_ENABLED=0 to benchmark the uncached baseline.
"""
import os
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException

from api.cache import TTLCache

SERVING = Path(os.getenv("SERVING_DIR", "data/lake/serving"))
CACHE_ENABLED = os.getenv("CACHE_ENABLED", "1") == "1"
cache = TTLCache(ttl_seconds=int(os.getenv("CACHE_TTL", "60")))

app = FastAPI(title="Game Telemetry Lakehouse API")


def _read(name, filters=None):
    # Deliberately re-reads from storage each call: this is the "cold" cost the cache removes.
    return pd.read_parquet(SERVING / name, filters=filters)


def _cached(key, loader):
    if CACHE_ENABLED:
        hit = cache.get(key)
        if hit is not None:
            return hit
    value = loader()
    if CACHE_ENABLED:
        cache.set(key, value)
    return value


@app.get("/health")
def health():
    return {"status": "ok", "cache_enabled": CACHE_ENABLED}


@app.get("/stats/genres")
def genres():
    return _cached("genres", lambda: _read("genre_stats").sort_values(
        "total_playtime_seconds", ascending=False).to_dict("records"))


@app.get("/stats/daily-active")
def daily_active():
    return _cached("dau", lambda: _read("daily_active").assign(
        event_date=lambda d: d["event_date"].astype(str)).sort_values("event_date").to_dict("records"))


@app.get("/players/{player_id}/stats")
def player_stats(player_id: str):
    def load():
        df = _read("player_stats", filters=[("player_id", "==", player_id)])
        if df.empty:
            return None
        top = _read("player_top_titles", filters=[("player_id", "==", player_id)])
        out = df.iloc[0].to_dict()
        out["top_titles"] = top.sort_values("playtime", ascending=False)[["title_id", "playtime"]].to_dict("records")
        return out

    result = _cached(f"player:{player_id}", load)
    if result is None:
        raise HTTPException(status_code=404, detail="player not found")
    return result


@app.get("/cache/stats")
def cache_stats():
    return cache.stats()
