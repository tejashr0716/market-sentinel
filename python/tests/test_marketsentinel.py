import time
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from python.api.main import app, get_engine
from python.services.cpp_bridge import MarketSentinelEngine
from python.database.connection import close_db_pool
from python.services.redis_service import (
    init_redis, close_redis, cache_market_tick, get_recent_ticks,
    record_exchange_heartbeat, trip_exchange_circuit, get_circuit_state
)
from python.services.rabbitmq_service import close_rabbitmq

@pytest.fixture(scope="module")
def cpp_engine():
    return MarketSentinelEngine()

@pytest_asyncio.fixture
async def async_client():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    await close_rabbitmq()
    await close_redis()
    await close_db_pool()

def test_cpp_engine_normal_tick(cpp_engine):
    res = cpp_engine.evaluate_tick(
        symbol="BTC-USD",
        exchange_id="BINANCE_US",
        price=95000.0,
        volume=1.5,
        bid=94998.0,
        ask=95002.0,
        baseline_mean=95000.0,
        baseline_std=200.0,
        baseline_vwap=94990.0,
        avg_volume=2.0
    )
    assert res["is_anomaly"] is False
    assert res["anomaly_type"] == "NONE"
    assert abs(res["z_score"]) < 1.0

def test_cpp_engine_flash_crash_detection(cpp_engine):
    res = cpp_engine.evaluate_tick(
        symbol="BTC-USD",
        exchange_id="BINANCE_US",
        price=89000.0, # Drop > 6%
        volume=50.0,
        bid=88990.0,
        ask=89010.0,
        baseline_mean=95000.0,
        baseline_std=300.0,
        baseline_vwap=94900.0,
        avg_volume=2.0
    )
    assert res["is_anomaly"] is True
    assert res["anomaly_type"] == "FLASH_CRASH"
    assert res["z_score"] < -3.0
    assert "Flash Crash" in res["reason"]

def test_cpp_engine_pump_and_dump_detection(cpp_engine):
    res = cpp_engine.evaluate_tick(
        symbol="ETH-USD",
        exchange_id="COINBASE_PRO",
        price=3600.0, # Spikes high
        volume=45.0,  # 9x avg volume
        bid=3598.0,
        ask=3602.0,
        baseline_mean=3300.0,
        baseline_std=40.0,
        baseline_vwap=3300.0,
        avg_volume=5.0
    )
    assert res["is_anomaly"] is True
    assert res["anomaly_type"] == "PUMP_AND_DUMP"
    assert res["z_score"] > 3.5

def test_cpp_engine_spread_inversion_detection(cpp_engine):
    res = cpp_engine.evaluate_tick(
        symbol="NVDA",
        exchange_id="NASDAQ_ITCH",
        price=140.0,
        volume=100.0,
        bid=141.50, # Bid > Ask!
        ask=139.50,
        baseline_mean=140.0,
        baseline_std=2.0,
        baseline_vwap=140.0,
        avg_volume=100.0
    )
    assert res["is_anomaly"] is True
    assert res["anomaly_type"] == "SPREAD_INVERSION"
    assert "Spread inversion" in res["reason"]

def test_cpp_engine_vwap_and_zscore_calculation(cpp_engine):
    prices = [100.0, 102.0, 101.0, 103.0, 105.0]
    volumes = [10.0, 20.0, 15.0, 25.0, 30.0]
    stats = cpp_engine.compute_vwap_and_zscore(prices, volumes)
    assert stats["vwap"] > 100.0
    assert isinstance(stats["z_score"], float)

def test_cpp_engine_batch_evaluation(cpp_engine):
    ticks = [
        {"symbol": "BTC-USD", "exchange_id": "BINANCE_US", "price": 95000.0 + i, "volume": 1.0, "bid": 94990.0, "ask": 95010.0}
        for i in range(10)
    ]
    baselines = [
        {"rolling_mean": 95000.0, "rolling_std_dev": 200.0, "rolling_vwap": 95000.0, "avg_volume": 1.0}
        for _ in range(10)
    ]
    results = cpp_engine.batch_evaluate(ticks, baselines)
    assert len(results) == 10
    for r in results:
        assert r["is_anomaly"] is False

@pytest.mark.asyncio
async def test_redis_tick_cache_and_heartbeats():
    await init_redis()
    await cache_market_tick("SOL-USD", 185.50, 12.0)
    await cache_market_tick("SOL-USD", 186.00, 15.0)

    recent = await get_recent_ticks("SOL-USD", limit=5)
    assert len(recent) >= 2
    assert recent[0]["price"] == 186.00

    await record_exchange_heartbeat("NASDAQ_ITCH")
    state = await get_circuit_state("NASDAQ_ITCH")
    assert state == "CLOSED"

    await trip_exchange_circuit("NASDAQ_ITCH")
    state_open = await get_circuit_state("NASDAQ_ITCH")
    assert state_open == "OPEN"
    await close_redis()

@pytest.mark.asyncio
async def test_api_health(async_client):
    resp = await async_client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "HEALTHY"
    assert "MarketSentinel" in data["service"]

@pytest.mark.asyncio
async def test_api_list_instruments(async_client):
    resp = await async_client.get("/api/v1/instruments")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) >= 4
    symbols = [i["symbol"] for i in data]
    assert "BTC-USD" in symbols
    assert "NVDA" in symbols

@pytest.mark.asyncio
async def test_api_list_exchanges(async_client):
    resp = await async_client.get("/api/v1/exchanges")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) >= 3
    ids = [e["exchange_id"] for e in data]
    assert "BINANCE_US" in ids

@pytest.mark.asyncio
async def test_api_tick_ingestion_and_circuit_breaker_lifecycle(async_client):
    # 1. Normal tick ingestion
    normal_payload = {
        "symbol": "BTC-USD",
        "exchange_id": "BINANCE_US",
        "price": 95500.0,
        "volume": 2.0,
        "bid": 95495.0,
        "ask": 95505.0
    }
    resp1 = await async_client.post("/api/v1/telemetry/tick", json=normal_payload)
    assert resp1.status_code == 201
    assert resp1.json()["status"] == "ACCEPTED"

    # 2. Flash Crash anomaly tick
    crash_payload = {
        "symbol": "BTC-USD",
        "exchange_id": "BINANCE_US",
        "price": 88000.0, # Drop > 7%
        "volume": 120.0,
        "bid": 87980.0,
        "ask": 88020.0
    }
    resp2 = await async_client.post("/api/v1/telemetry/tick", json=crash_payload)
    assert resp2.status_code == 201
    data2 = resp2.json()
    assert data2["is_anomaly"] is True
    assert data2["anomaly_type"] == "FLASH_CRASH"

    # 3. Simulate Exchange Failure (Kill-switch)
    kill_resp = await async_client.post("/api/v1/exchanges/kill", json={"exchange_id": "BINANCE_US"})
    assert kill_resp.status_code == 200
    assert kill_resp.json()["status"] == "CIRCUIT_OPEN"

    # 4. Ingestion must now be rejected by Circuit Breaker (503 Service Unavailable)
    rejected_resp = await async_client.post("/api/v1/telemetry/tick", json=normal_payload)
    assert rejected_resp.status_code == 503

    # 5. Heartbeat restores circuit
    hb_resp = await async_client.post("/api/v1/exchanges/heartbeat", json={"exchange_id": "BINANCE_US"})
    assert hb_resp.status_code == 200
    assert hb_resp.json()["circuit"] == "CLOSED"

    # 6. Ingestion succeeds again
    resp_recovered = await async_client.post("/api/v1/telemetry/tick", json=normal_payload)
    assert resp_recovered.status_code == 201

@pytest.mark.asyncio
async def test_api_anomalies_list(async_client):
    resp = await async_client.get("/api/v1/anomalies")
    assert resp.status_code == 200
    data = resp.json()
    assert "anomalies" in data
    assert "dropouts" in data
    assert len(data["anomalies"]) > 0

def test_spark_forensics_import():
    from python.spark.spark_market_forensics import run_market_forensics_analysis, build_spark_session
    assert callable(run_market_forensics_analysis)
    assert callable(build_spark_session)
