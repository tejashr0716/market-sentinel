import asyncio
import random
import time
import statistics
from httpx import AsyncClient, ASGITransport
from python.api.main import app
from python.database.connection import close_db_pool
from python.services.redis_service import close_redis, record_exchange_heartbeat
from python.services.rabbitmq_service import close_rabbitmq

TOTAL_TICKS = 1200
CONCURRENCY = 15
SYMBOLS = ["BTC-USD", "ETH-USD", "NVDA", "AAPL", "SOL-USD"]
EXCHANGES = ["BINANCE_US", "COINBASE_PRO", "NASDAQ_ITCH"]

BASE_PRICES = {
    "BTC-USD": 95000.0,
    "ETH-USD": 3350.0,
    "NVDA": 135.0,
    "AAPL": 225.0,
    "SOL-USD": 185.0
}

async def worker(client: AsyncClient, queue: asyncio.Queue, latencies: list):
    while True:
        try:
            payload = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        t0 = time.perf_counter()
        resp = await client.post("/api/v1/telemetry/tick", json=payload)
        t1 = time.perf_counter()
        if resp.status_code == 201:
            latencies.append((t1 - t0) * 1000.0)

async def run_stress_test():
    print("=================================================================")
    print("  MarketSentinel Financial Telemetry Stress Test                 ")
    print(f"  Target: {TOTAL_TICKS} ticks | Concurrency: {CONCURRENCY} workers")
    print("=================================================================\n")

    # Reset all circuits to CLOSED before test
    for exch in EXCHANGES:
        await record_exchange_heartbeat(exch)

    queue = asyncio.Queue()
    now_ns = int(time.time() * 1e9)

    for i in range(TOTAL_TICKS):
        sym = random.choice(SYMBOLS)
        base = BASE_PRICES[sym]
        exch = random.choice(EXCHANGES)

        # 95% normal drift, 5% anomaly spike
        is_spike = (random.random() < 0.05)
        if is_spike:
            price = base * (0.92 if random.random() < 0.5 else 1.08)
            vol = random.uniform(50.0, 200.0)
        else:
            price = base + random.uniform(-base * 0.005, base * 0.005)
            vol = random.uniform(0.5, 10.0)

        bid = price - 0.05
        ask = price + 0.05

        queue.put_nowait({
            "symbol": sym,
            "exchange_id": exch,
            "price": round(price, 2),
            "volume": round(vol, 4),
            "bid": round(bid, 2),
            "ask": round(ask, 2),
            "timestamp_ns": now_ns + i * 1000,
            "sequence_no": i
        })

    latencies = []
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        start_time = time.perf_counter()
        tasks = [asyncio.create_task(worker(client, queue, latencies)) for _ in range(CONCURRENCY)]
        await asyncio.gather(*tasks)
        total_time = time.perf_counter() - start_time

    await close_rabbitmq()
    await close_redis()
    await close_db_pool()

    latencies.sort()
    throughput = len(latencies) / total_time
    p50 = statistics.median(latencies)
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]

    print(f">> Completed:            {len(latencies)} / {TOTAL_TICKS} ticks processed")
    print(f">> Total Wall Time:      {total_time:.2f} seconds")
    print(f">> Sustained Throughput: {throughput:,.1f} ticks/sec")
    print(f">> Latency p50:          {p50:.2f} ms")
    print(f">> Latency p95:          {p95:.2f} ms")
    print(f">> Latency p99:          {p99:.2f} ms")
    print("=================================================================")

if __name__ == "__main__":
    asyncio.run(run_stress_test())
