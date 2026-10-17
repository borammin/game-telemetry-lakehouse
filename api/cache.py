import time
from threading import Lock


class TTLCache:
    """Small thread-safe TTL cache with hit/miss counters."""

    def __init__(self, ttl_seconds=60, max_items=10_000):
        self.ttl = ttl_seconds
        self.max_items = max_items
        self._data = {}
        self._lock = Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key):
        with self._lock:
            item = self._data.get(key)
            if item and item[0] > time.monotonic():
                self.hits += 1
                return item[1]
            self._data.pop(key, None)
            self.misses += 1
            return None

    def set(self, key, value):
        with self._lock:
            if len(self._data) >= self.max_items:
                self._data.pop(next(iter(self._data)))
            self._data[key] = (time.monotonic() + self.ttl, value)

    def clear(self):
        with self._lock:
            self._data.clear()
            self.hits = self.misses = 0

    def stats(self):
        total = self.hits + self.misses
        return {"hits": self.hits, "misses": self.misses,
                "hit_rate": round(self.hits / total, 4) if total else 0.0}
