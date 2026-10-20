import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from python.database.connection import init_db_pool, close_db_pool, get_db_pool
from python.services.cpp_bridge import MarketSentinelEngine
from python.services.redis_service import (
    init_redis, close_redis, cache_market_tick, get_recent_ticks,
    record_exchange_heartbeat, get_exchange_heartbeat, trip_exchange_circuit,
    get_circuit_state, publish_anomaly_alert
)
from python.services.rabbitmq_service import init_rabbitmq, close_rabbitmq, publish_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("marketsentinel.api")

engine: Optional[MarketSentinelEngine] = None

def get_engine() -> MarketSentinelEngine:
    global engine
    if engine is None:
        engine = MarketSentinelEngine()
    return engine

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket client connected. Total clients: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket client disconnected. Total clients: {len(self.active_connections)}")

    async def broadcast(self, message: Dict[str, Any]):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

ws_manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing MarketSentinel Financial Surveillance Gateway...")
    await init_db_pool()
    await init_redis()
    await init_rabbitmq()
    get_engine()
    logger.info("MarketSentinel Core services initialized and ready.")
    yield
    logger.info("Shutting down MarketSentinel Services...")
    await close_rabbitmq()
    await close_redis()
    await close_db_pool()

app = FastAPI(
    title="MarketSentinel High-Throughput Financial Anomaly & Surveillance Gateway",
    version="1.0.0",
    description="Sub-Microsecond Market Telemetry Ingestion, Z-Score Anomaly Detection & Circuit Breaker Engine",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request Models
class MarketTickPayload(BaseModel):
    symbol: str
    exchange_id: str
    price: float
    volume: float
    bid: float
    ask: float
    timestamp_ns: Optional[int] = None
    sequence_no: Optional[int] = 0

class HeartbeatPayload(BaseModel):
    exchange_id: str

@app.get("/health")
async def health_check():
    return {
        "status": "HEALTHY",
        "service": "MarketSentinel Financial Surveillance Engine",
        "engine": "C++ SIMD Welford & VWAP Core Loaded",
        "active_websockets": len(ws_manager.active_connections)
    }

@app.get("/api/v1/instruments")
async def list_instruments():
    db = await get_db_pool()
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM instruments ORDER BY symbol ASC;")
        return [dict(r) for r in rows]

@app.get("/api/v1/exchanges")
async def list_exchanges():
    db = await get_db_pool()
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM exchanges ORDER BY exchange_id ASC;")
        output = []
        for r in rows:
            circuit = await get_circuit_state(r["exchange_id"])
            item = dict(r)
            item["circuit_state"] = circuit
            output.append(item)
        return output

@app.post("/api/v1/telemetry/tick", status_code=status.HTTP_201_CREATED)
async def ingest_market_tick(payload: MarketTickPayload):
    eng = get_engine()
    now_ns = payload.timestamp_ns or int(time.time() * 1e9)

    # Check exchange circuit breaker
    circuit = await get_circuit_state(payload.exchange_id)
    if circuit == "OPEN":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Exchange {payload.exchange_id} is in TRIP/OPEN circuit state due to dropouts."
        )

    # 1. Fetch prior ticks from Redis cache for historical baseline
    recent = await get_recent_ticks(payload.symbol, limit=30)
    if recent:
        prices = [t["price"] for t in recent]
        volumes = [t["volume"] for t in recent]
        mean = sum(prices) / len(prices)
        variance = sum((p - mean) ** 2 for p in prices) / len(prices)
        std_dev = max(50.0 if "BTC" in payload.symbol else 0.5, variance ** 0.5)
        avg_vol = max(0.1, sum(volumes) / len(volumes))
    else:
        prices = [payload.price]
        volumes = [payload.volume]
        mean = payload.price
        std_dev = 100.0 if "BTC" in payload.symbol else 2.0
        avg_vol = payload.volume

    stats = eng.compute_vwap_and_zscore(prices + [payload.price], volumes + [payload.volume])

    # 2. Evaluate tick with C++ high-speed core
    eval_result = eng.evaluate_tick(
        symbol=payload.symbol,
        exchange_id=payload.exchange_id,
        price=payload.price,
        volume=payload.volume,
        bid=payload.bid,
        ask=payload.ask,
        baseline_mean=mean,
        baseline_std=std_dev,
        baseline_vwap=stats["vwap"],
        avg_volume=avg_vol,
        timestamp_ns=now_ns,
        seq_no=payload.sequence_no or 0
    )

    # 3. Update Redis cache
    await cache_market_tick(payload.symbol, payload.price, payload.volume)
    await record_exchange_heartbeat(payload.exchange_id)

    # 4. If Anomaly detected, write incident to DB & publish alert
    db = await get_db_pool()
    async with db.acquire() as conn:
        # Write tick log
        await conn.execute(
            """
            INSERT INTO market_ticks (symbol, exchange_id, price, volume, bid, ask, vwap, z_score)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
            """,
            payload.symbol, payload.exchange_id, payload.price, payload.volume,
            payload.bid, payload.ask, stats["vwap"], eval_result["z_score"]
        )

        if eval_result["is_anomaly"]:
            severity = "CRITICAL" if eval_result["anomaly_type"] in ["FLASH_CRASH", "PUMP_AND_DUMP"] else "HIGH"
            action = "QUARANTINED" if severity == "CRITICAL" else "ALERTED"

            row = await conn.fetchrow(
                """
                INSERT INTO anomaly_incidents (symbol, exchange_id, anomaly_type, severity, trigger_price, baseline_price, deviation_pct, trigger_volume, action_taken, raw_telemetry)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                RETURNING incident_id;
                """,
                payload.symbol, payload.exchange_id, eval_result["anomaly_type"], severity,
                payload.price, mean, eval_result["deviation_pct"], payload.volume, action,
                json.dumps({"z_score": eval_result["z_score"], "reason": eval_result["reason"]})
            )

            alert_event = {
                "type": "ANOMALY_ALERT",
                "incident_id": row["incident_id"],
                "symbol": payload.symbol,
                "exchange_id": payload.exchange_id,
                "anomaly_type": eval_result["anomaly_type"],
                "severity": severity,
                "price": payload.price,
                "z_score": eval_result["z_score"],
                "reason": eval_result["reason"],
                "timestamp": time.time()
            }
            await publish_anomaly_alert(alert_event)
            await publish_event("market.anomalies.critical", alert_event)
            asyncio.create_task(ws_manager.broadcast(alert_event))

    return {
        "status": "ACCEPTED",
        "symbol": payload.symbol,
        "price": payload.price,
        "vwap": stats["vwap"],
        "is_anomaly": eval_result["is_anomaly"],
        "anomaly_type": eval_result["anomaly_type"],
        "z_score": eval_result["z_score"],
        "latency_ns": eval_result["latency_ns"]
    }

@app.post("/api/v1/exchanges/heartbeat")
async def exchange_heartbeat(payload: HeartbeatPayload):
    await record_exchange_heartbeat(payload.exchange_id)
    db = await get_db_pool()
    async with db.acquire() as conn:
        await conn.execute(
            """
            UPDATE exchanges
            SET last_heartbeat_at = NOW(), consecutive_missed_heartbeats = 0, status = 'OPERATIONAL'
            WHERE exchange_id = $1;
            """,
            payload.exchange_id
        )
    return {"exchange_id": payload.exchange_id, "status": "OPERATIONAL", "circuit": "CLOSED"}

@app.post("/api/v1/exchanges/kill")
async def simulate_exchange_failure(payload: HeartbeatPayload):
    # Simulate feed dropout / trip circuit
    await trip_exchange_circuit(payload.exchange_id)
    db = await get_db_pool()
    async with db.acquire() as conn:
        await conn.execute(
            """
            UPDATE exchanges
            SET status = 'CIRCUIT_OPEN', consecutive_missed_heartbeats = 5
            WHERE exchange_id = $1;
            """,
            payload.exchange_id
        )
        await conn.execute(
            """
            INSERT INTO exchange_dropout_events (exchange_id, failure_type, missed_heartbeats, duration_ms, circuit_breaker_state)
            VALUES ($1, 'SIMULATED_DROPOUT', 5, 2500, 'OPEN');
            """,
            payload.exchange_id
        )

    dropout_event = {
        "type": "EXCHANGE_DROPOUT",
        "exchange_id": payload.exchange_id,
        "status": "CIRCUIT_TRIPPED",
        "reason": "Excessive heartbeat delay / manual kill",
        "timestamp": time.time()
    }
    await publish_event("market.dropouts.alert", dropout_event)
    asyncio.create_task(ws_manager.broadcast(dropout_event))

    return {"exchange_id": payload.exchange_id, "status": "CIRCUIT_OPEN", "alert": "CIRCUIT_TRIPPED"}

@app.get("/api/v1/anomalies")
async def get_anomalies():
    db = await get_db_pool()
    async with db.acquire() as conn:
        anomalies = await conn.fetch("SELECT * FROM anomaly_incidents ORDER BY incident_id DESC LIMIT 20;")
        dropouts = await conn.fetch("SELECT * FROM exchange_dropout_events ORDER BY dropout_id DESC LIMIT 10;")
        return {
            "anomalies": [
                {
                    "incident_id": a["incident_id"],
                    "symbol": a["symbol"],
                    "exchange_id": a["exchange_id"],
                    "anomaly_type": a["anomaly_type"],
                    "severity": a["severity"],
                    "trigger_price": a["trigger_price"],
                    "deviation_pct": a["deviation_pct"],
                    "action_taken": a["action_taken"],
                    "detected_at": a["detected_at"].isoformat()
                }
                for a in anomalies
            ],
            "dropouts": [
                {
                    "dropout_id": d["dropout_id"],
                    "exchange_id": d["exchange_id"],
                    "failure_type": d["failure_type"],
                    "circuit_breaker_state": d["circuit_breaker_state"],
                    "detected_at": d["detected_at"].isoformat()
                }
                for d in dropouts
            ]
        }

@app.websocket("/ws/stream")
async def websocket_stream(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        await websocket.send_json({
            "type": "CONNECTED",
            "message": "MarketSentinel Real-Time Anomaly Stream Active"
        })
        while True:
            msg = await websocket.receive_text()
            if msg == "ping":
                await websocket.send_json({"type": "PONG"})
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)
