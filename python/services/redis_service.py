import json
import os
import time
import redis.asyncio as redis
from typing import Optional, Dict, Any, List

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "10"))

redis_client: Optional[redis.Redis] = None

async def init_redis() -> redis.Redis:
    global redis_client
    if redis_client is not None:
        try:
            await redis_client.aclose()
        except Exception:
            pass
        redis_client = None

    redis_client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )
    return redis_client

async def get_redis() -> redis.Redis:
    global redis_client
    if redis_client is None:
        await init_redis()
    return redis_client

async def close_redis():
    global redis_client
    if redis_client is not None:
        try:
            await redis_client.aclose()
        except Exception:
            pass
        redis_client = None

async def cache_market_tick(symbol: str, price: float, volume: float, max_window: int = 100):
    client = await get_redis()
    now_ms = int(time.time() * 1000)
    key = f"sentinel:ticks:{symbol}"
    val = f"{price}:{volume}:{now_ms}"
    await client.lpush(key, val)
    await client.ltrim(key, 0, max_window - 1)

async def get_recent_ticks(symbol: str, limit: int = 50) -> List[Dict[str, float]]:
    client = await get_redis()
    key = f"sentinel:ticks:{symbol}"
    raw = await client.lrange(key, 0, limit - 1)
    output = []
    for item in raw:
        try:
            p, v, ts = item.split(":")
            output.append({"price": float(p), "volume": float(v), "timestamp_ms": int(ts)})
        except Exception:
            continue
    return output

async def record_exchange_heartbeat(exchange_id: str):
    client = await get_redis()
    now_ms = int(time.time() * 1000)
    await client.set(f"sentinel:heartbeat:{exchange_id}", str(now_ms))
    await client.set(f"sentinel:circuit:{exchange_id}", "CLOSED")

async def get_exchange_heartbeat(exchange_id: str) -> Optional[int]:
    client = await get_redis()
    val = await client.get(f"sentinel:heartbeat:{exchange_id}")
    return int(val) if val else None

async def trip_exchange_circuit(exchange_id: str):
    client = await get_redis()
    await client.set(f"sentinel:circuit:{exchange_id}", "OPEN")

async def get_circuit_state(exchange_id: str) -> str:
    client = await get_redis()
    val = await client.get(f"sentinel:circuit:{exchange_id}")
    return val if val else "CLOSED"

async def publish_anomaly_alert(alert: Dict[str, Any]):
    client = await get_redis()
    await client.publish("sentinel:market_alerts", json.dumps(alert))
