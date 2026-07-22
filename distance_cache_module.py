"""
distance_cache_module.py
--------------------------
Persistent cache for OSRM driving-distance results, keyed by a rounded
farmer location + mandi name. Roads don't move, so a cached result is
valid indefinitely - no TTL expiry.

Why this exists: repeatedly calling OSRM for the same (or nearby) farmer
location asking about the same mandi is pure waste - it costs latency,
risks OSRM's rate limit, and the answer never changes. This cache turns
routing from "a live external dependency on every request" into "an
instant lookup after the first request", which is the actual fix for
concurrent-user load, not just a bigger sleep().

Grid snapping: farmer lat/lon is rounded to 2 decimal places (~1.1km
grid cells). Two farmers within the same cell share a cached distance to
a given mandi - a reasonable approximation given profit differences
between mandis are usually far larger than 1km of routing error.

SQLite is fine for a single-process deployment (the common case for a
prototype/early-stage API). For multi-worker or multi-replica production
deployments, swap this for a shared store (Postgres, Redis) so all
processes see the same cache - noted explicitly, not silently glossed
over.
"""

import sqlite3
import pathlib
import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = pathlib.Path(__file__).parent / "mandi_distance_cache.db"
GRID_DECIMALS = 2  # ~1.1km grid cells


def _init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS distance_cache (
            cache_key TEXT PRIMARY KEY,
            distance_km REAL NOT NULL,
            source TEXT NOT NULL,
            cached_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


_init_db()


def _make_key(lat: float, lon: float, market_name: str) -> str:
    glat = round(lat, GRID_DECIMALS)
    glon = round(lon, GRID_DECIMALS)
    return f"{glat}:{glon}:{market_name.strip().lower()}"


def _get_sync(lat: float, lon: float, market_name: str) -> Optional[dict]:
    key = _make_key(lat, lon, market_name)
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute(
            "SELECT distance_km, source, cached_at FROM distance_cache WHERE cache_key = ?",
            (key,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    return {"distance_km": row[0], "source": row[1], "cached_at": row[2]}


def _set_sync(lat: float, lon: float, market_name: str, distance_km: float, source: str):
    key = _make_key(lat, lon, market_name)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            "INSERT OR REPLACE INTO distance_cache (cache_key, distance_km, source) VALUES (?, ?, ?)",
            (key, distance_km, source),
        )
        conn.commit()
    finally:
        conn.close()


async def get_cached_distance(lat: float, lon: float, market_name: str) -> Optional[dict]:
    """Returns {distance_km, source, cached_at} or None on cache miss."""
    return await asyncio.to_thread(_get_sync, lat, lon, market_name)


async def set_cached_distance(lat: float, lon: float, market_name: str, distance_km: float, source: str):
    """
    Only cache real OSRM results (source="osrm"), never the haversine
    fallback - if OSRM later becomes available we want a real distance
    cached, not a permanently-stuck approximation.
    """
    if source != "osrm":
        return
    await asyncio.to_thread(_set_sync, lat, lon, market_name, distance_km, source)


async def cache_stats() -> dict:
    """Quick visibility into cache size - useful for a monitoring endpoint."""
    def _count():
        conn = sqlite3.connect(DB_PATH)
        try:
            n = conn.execute("SELECT COUNT(*) FROM distance_cache").fetchone()[0]
        finally:
            conn.close()
        return n
    total = await asyncio.to_thread(_count)
    return {"cached_pairs": total, "db_path": str(DB_PATH)}