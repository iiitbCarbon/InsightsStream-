"""
InsightsStream — FastAPI serving layer.

Exposes low-latency, async read endpoints over the aggregated metrics in
PostgreSQL. Hot queries are cached in Redis to keep dashboard reads sub-ms.
"""
from datetime import datetime

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, Query

app = FastAPI(title="InsightsStream API", version="1.0.0")

DB_DSN = "postgresql://insights:insights@localhost:5432/insights"
CACHE_TTL = 30  # seconds

pool: asyncpg.Pool | None = None
cache: redis.Redis | None = None


@app.on_event("startup")
async def startup() -> None:
    global pool, cache
    pool = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=10)
    cache = redis.from_url("redis://localhost:6379", decode_responses=True)


@app.on_event("shutdown")
async def shutdown() -> None:
    if pool:
        await pool.close()
    if cache:
        await cache.aclose()


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/metrics")
async def get_metrics(
    metric: str = Query(..., description="Metric name, e.g. page_views"),
    window: datetime = Query(..., description="Window start (ISO 8601)"),
) -> dict:
    cache_key = f"metric:{metric}:{window.isoformat()}"

    cached = await cache.get(cache_key)
    if cached is not None:
        return {"metric": metric, "window_start": window, "total": int(cached), "cached": True}

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT total FROM metric_aggregates
            WHERE metric = $1 AND window_start = $2
            """,
            metric,
            window,
        )

    total = int(row["total"]) if row else 0
    await cache.set(cache_key, total, ex=CACHE_TTL)
    return {"metric": metric, "window_start": window, "total": total, "cached": False}
