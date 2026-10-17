import time

from api.cache import TTLCache


def test_hit_and_miss_counting():
    c = TTLCache(ttl_seconds=5)
    assert c.get("a") is None
    c.set("a", 1)
    assert c.get("a") == 1
    assert c.stats() == {"hits": 1, "misses": 1, "hit_rate": 0.5}


def test_expiry():
    c = TTLCache(ttl_seconds=0.05)
    c.set("a", 1)
    time.sleep(0.1)
    assert c.get("a") is None


def test_eviction_at_capacity():
    c = TTLCache(ttl_seconds=5, max_items=2)
    c.set("a", 1)
    c.set("b", 2)
    c.set("c", 3)
    assert c.get("a") is None
    assert c.get("c") == 3
