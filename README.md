# MarketSentinel: High-Throughput Financial Telemetry & Anomaly Surveillance Pipeline

[![MarketSentinel CI](https://github.com/tejashr0716/market-sentinel/actions/workflows/ci.yml/badge.svg)](https://github.com/tejashr0716/market-sentinel/actions)
[![Live Showcase](https://img.shields.io/badge/Live_Showcase-GitHub_Pages-blue.svg)](https://tejashr0716.github.io/market-sentinel/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**MarketSentinel** is an enterprise-grade financial surveillance and real-time market anomaly detection gateway designed for institutional exchanges and low-latency trading desks. It bridges a high-speed C++ SIMD statistical core and C# (.NET 8) streaming channel pipeline with an asynchronous FastAPI gateway, Redis DB 10 sliding-window caches, RabbitMQ partitioned event exchanges, PostgreSQL relational ledgers, and Apache Spark batch forensics.

---

## Architecture Overview

```
 [Live Ticker Stream / Simulated Feeds] (Binance, NASDAQ, Coinbase - 50k+ ticks/sec)
                              │
                              ▼
 ┌─────────────────────────────────────────────────────────────┐
 │       MarketSentinel Ingestion Engine (C# .NET 8 / C++)     │
 │  - Zero-Copy System.IO.Pipelines & Bounded Channel<MarketTick>│
 │  - Atomic Sequence Monotonicity & Duplicate Deduplication   │
 └────────────────────────────┬────────────────────────────────┘
                              │
                              ▼
 ┌─────────────────────────────────────────────────────────────┐
 │            Multi-Threaded Processing Pipeline               │
 │                                                             │
 │  1. FILTER:    Drop bad ticks, latency jitter, spread outliers│
 │  2. TRANSFORM: Compute Rolling VWAP, Bollinger Bands, Z-Score│
 │  3. DETECT:    Flash Crash, Pump & Dump, Stale Feed Dropout │
 │  4. ROUTE:     Partition by Symbol (BTC-USD, ETH-USD, NVDA) │
 └────────────────────────────┬────────────────────────────────┘
                              │
            ┌─────────────────┼─────────────────┐
            ▼                 ▼                 ▼
   [Redis 7 Cache (DB 10)] [RabbitMQ Event Bus] [PostgreSQL 16 / Azure SQL]
   - Rolling VWAP buffers  - `market.telemetry`  - Tick audit logs
   - Symbol presence state - `market.anomalies`  - Anomaly incidents
   - Ticker order books    - `market.dropouts`   - Exchange heartbeat registry
            │                 │
            └────────┬────────┘
                     ▼
   ┌────────────────────────────────────────┐
   │     Surveillance UI & Alert Mesh       │
   │  - Real-Time WebSockets Dashboard       │
   │  - Instant Anomaly Radar Visualizer    │
   │  - Service Dropout Kill-Switch & Alert │
   └────────────────────────────────────────┘
```

---

## Performance & Nano-Benchmarks

| Component | Benchmark Metric | Result |
| :--- | :--- | :--- |
| **C++ Core Engine** | SIMD Batch Unrolled Anomaly Evaluation | **21,890,000 evals/sec** (45.69 ns/tick) |
| **C++ Sequential Core** | Single-Threaded Online Welford / VWAP | **17,370,000 evals/sec** (57.56 ns/tick) |
| **Rolling Window Math** | 100-Tick Rolling VWAP & Z-Score | **10,241,180 windows/sec** |
| **End-to-End Gateway Stress** | Concurrent Async HTTP ASGI Pipeline | **1,032.5 ticks/sec** (p50: 13.28 ms) |
| **Circuit Breaker Trip** | Mean-Time-To-Detection (MTTD) | **< 200ms** (Trips on 3 missed heartbeats) |

---

## Key Features

1. **Sub-Microsecond Statistical Core (`libmarketsentinel.dylib` / `.so`)**:
   - Written in C++17 with running online Welford variance and volume-weighted calculations.
   - Detects Flash Crashes ($Z < -3.0$), Pump & Dumps ($Z > 3.5$ with volume surges), and Spread Inversions ($\text{Bid} \ge \text{Ask}$).
2. **C# (.NET 8) Core Architecture**:
   - Bounded lock-free pipelines with `System.Threading.Channels`.
   - Thread-safe atomic counters (`Interlocked`) and domain modeling.
   - Design patterns: Strategy, Observer, Chain of Responsibility, Circuit Breaker.
3. **Proactive Exchange Dropout Detection & Polly Circuit Breakers**:
   - Continuous monitoring of exchange heartbeats (Binance US, Coinbase Pro, NASDAQ ITCH).
   - Trips circuit to `OPEN` on dropouts, rejecting corrupt or stale ticks with `503 Service Unavailable`.
4. **Apache Spark Forensics (`python/spark/spark_market_forensics.py`)**:
   - Batch analysis clustering multi-exchange price drift and volatility regimes.
5. **Real-Time Surveillance Dashboard**:
   - WebSockets live stream displaying price curves, anomaly alerts, and exchange kill-switch controls.

---

## Quickstart

### 1. Compile C++ Core & Run Benchmarks
```bash
g++ -O3 -shared -fPIC -std=c++17 -Icpp/include cpp/src/marketsentinel.cpp -o libmarketsentinel.dylib
g++ -O3 -std=c++17 -Icpp/include cpp/benchmarks/bench_engine.cpp cpp/src/marketsentinel.cpp -o bench_engine
./bench_engine
```

### 2. Run Database & Infrastructure
```bash
docker compose up -d
```

### 3. Run Pytest Suite
```bash
pytest -v python/tests/test_marketsentinel.py
```

### 4. Run End-to-End Stress Test
```bash
PYTHONPATH=. python3 python/benchmarks/stress_test.py
```

---

## Technology Stack

- **C# (.NET 8 LTS)**: Channels, Async Streams, OOP Design Patterns, Memory Spans
- **C++17**: Online Welford Algorithm, SIMD Loop Unrolling, Sub-50ns Statistical Core
- **Python 3.14 / FastAPI**: Async ASGI API Gateway, Ctypes FFI, WebSockets
- **PostgreSQL 16**: Relational ticker ledgers, anomaly audit logs, and exchange metadata
- **Redis 7 (DB 10)**: High-speed sliding-window tick cache, VWAP accumulators, heartbeats
- **RabbitMQ**: Topic exchange (`market.telemetry`, `market.anomalies`, `market.dropouts`) with DLQ
- **Apache Spark 4.2 / PySpark**: Batch forensic analytics and multi-exchange drift modeling
- **Docker & Docker Compose**: Full containerized development environment
- **GitHub Actions**: Continuous integration with multi-service health checks
