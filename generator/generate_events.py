"""Generate synthetic player telemetry as newline-delimited JSON (the "raw" source).

Usage: python generator/generate_events.py --rows 5000000 --out data/raw
"""
import argparse
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

GENRES = ["Action", "RPG", "Shooter", "Racing", "Sports", "Puzzle", "Strategy", "Adventure"]
EVENT_TYPES = ["session_start", "session_end", "achievement", "purchase"]
PLATFORMS = ["console", "pc", "cloud", "mobile"]


def build_catalog(n_titles=500, seed=7):
    rng = random.Random(seed)
    return [{"title_id": f"T{i:04d}", "genre": rng.choice(GENRES)} for i in range(n_titles)]


def generate(rows, out_dir, n_players=200_000, files=20, seed=42):
    rng = random.Random(seed)
    catalog = build_catalog()
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    per_file = rows // files
    for f in range(files):
        with open(out / f"events_{f:03d}.jsonl", "w") as fh:
            for _ in range(per_file):
                title = rng.choice(catalog)
                ts = start + timedelta(seconds=rng.randint(0, 120 * 24 * 3600))
                rec = {
                    "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
                    "player_id": f"P{rng.randint(1, n_players):07d}",
                    "title_id": title["title_id"],
                    "genre": title["genre"],
                    "event_type": rng.choice(EVENT_TYPES),
                    "platform": rng.choice(PLATFORMS),
                    "playtime_seconds": rng.randint(60, 4 * 3600),
                    "event_ts": ts.isoformat(),
                }
                # ~1% dirty rows so the silver layer has real cleaning work to do
                roll = rng.random()
                if roll < 0.005:
                    rec["player_id"] = None
                elif roll < 0.010:
                    rec["playtime_seconds"] = -1
                fh.write(json.dumps(rec) + "\n")
                # ~0.5% duplicates to exercise dedup
                if rng.random() < 0.005:
                    fh.write(json.dumps(rec) + "\n")
    print(f"wrote ~{rows:,} events to {out}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=1_000_000)
    p.add_argument("--out", default="data/raw")
    a = p.parse_args()
    generate(a.rows, a.out)
