import importlib

import pandas as pd
import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    serving = tmp_path / "serving"
    for name, rows in {
        "genre_stats": [{"genre": "RPG", "total_playtime_seconds": 100, "players": 2},
                        {"genre": "Action", "total_playtime_seconds": 300, "players": 3}],
        "daily_active": [{"event_date": "2026-01-02", "active_players": 5},
                         {"event_date": "2026-01-01", "active_players": 4}],
        "player_stats": [{"player_id": "P1", "total_playtime_seconds": 500,
                          "distinct_titles": 2, "events": 4}],
        "player_top_titles": [{"player_id": "P1", "title_id": "T1", "playtime": 300},
                              {"player_id": "P1", "title_id": "T2", "playtime": 200}],
    }.items():
        (serving / name).mkdir(parents=True)
        pd.DataFrame(rows).to_parquet(serving / name / "part.parquet")
    monkeypatch.setenv("SERVING_DIR", str(serving))
    monkeypatch.setenv("CACHE_ENABLED", "1")
    import api.main as main
    importlib.reload(main)
    return TestClient(main.app), main


def test_health(client):
    c, _ = client
    assert c.get("/health").json()["status"] == "ok"


def test_genres_sorted_desc(client):
    c, _ = client
    assert [g["genre"] for g in c.get("/stats/genres").json()] == ["Action", "RPG"]


def test_player_stats_and_cache_hit(client):
    c, main = client
    first = c.get("/players/P1/stats").json()
    assert first["total_playtime_seconds"] == 500
    assert first["top_titles"][0]["title_id"] == "T1"
    c.get("/players/P1/stats")
    assert main.cache.stats()["hits"] >= 1


def test_unknown_player_404(client):
    c, _ = client
    assert c.get("/players/NOPE/stats").status_code == 404
