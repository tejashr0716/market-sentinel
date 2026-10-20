-- MarketSentinel: High-Throughput Financial Telemetry & Anomaly Surveillance Schema

DROP TABLE IF EXISTS exchange_dropout_events CASCADE;
DROP TABLE IF EXISTS anomaly_incidents CASCADE;
DROP TABLE IF EXISTS market_ticks CASCADE;
DROP TABLE IF EXISTS instruments CASCADE;
DROP TABLE IF EXISTS exchanges CASCADE;

CREATE TABLE exchanges (
    exchange_id VARCHAR(32) PRIMARY KEY,
    exchange_name VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'OPERATIONAL', -- OPERATIONAL, DEGRADED, OUTAGE, CIRCUIT_OPEN
    heartbeat_interval_ms INT NOT NULL DEFAULT 500,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    consecutive_missed_heartbeats INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE instruments (
    symbol VARCHAR(32) PRIMARY KEY,
    asset_class VARCHAR(32) NOT NULL, -- CRYPTO, EQUITIES, FOREX, COMMODITIES
    base_currency VARCHAR(16) NOT NULL,
    quote_currency VARCHAR(16) NOT NULL,
    tick_size DOUBLE PRECISION NOT NULL DEFAULT 0.01,
    min_volume DOUBLE PRECISION NOT NULL DEFAULT 0.0001,
    is_active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE market_ticks (
    tick_id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL REFERENCES instruments(symbol) ON DELETE CASCADE,
    exchange_id VARCHAR(32) NOT NULL REFERENCES exchanges(exchange_id) ON DELETE CASCADE,
    price DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION NOT NULL,
    bid DOUBLE PRECISION NOT NULL,
    ask DOUBLE PRECISION NOT NULL,
    vwap DOUBLE PRECISION,
    z_score DOUBLE PRECISION,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE anomaly_incidents (
    incident_id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL REFERENCES instruments(symbol) ON DELETE CASCADE,
    exchange_id VARCHAR(32) NOT NULL REFERENCES exchanges(exchange_id) ON DELETE CASCADE,
    anomaly_type VARCHAR(64) NOT NULL, -- FLASH_CRASH, PUMP_AND_DUMP, VOLATILITY_SPIKE, SPREAD_INVERSION, WASH_TRADING
    severity VARCHAR(32) NOT NULL,    -- LOW, MEDIUM, HIGH, CRITICAL
    trigger_price DOUBLE PRECISION NOT NULL,
    baseline_price DOUBLE PRECISION NOT NULL,
    deviation_pct DOUBLE PRECISION NOT NULL,
    trigger_volume DOUBLE PRECISION NOT NULL,
    action_taken VARCHAR(64) NOT NULL, -- QUARANTINED, ALERTED, CIRCUIT_TRIPPED
    raw_telemetry JSONB NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE exchange_dropout_events (
    dropout_id BIGSERIAL PRIMARY KEY,
    exchange_id VARCHAR(32) NOT NULL REFERENCES exchanges(exchange_id) ON DELETE CASCADE,
    failure_type VARCHAR(64) NOT NULL, -- MISSED_HEARTBEATS, CONNECTION_DROPOUT, LATENCY_SPIKE, INVALID_CHECKSUM
    missed_heartbeats INT NOT NULL,
    duration_ms INT NOT NULL DEFAULT 0,
    circuit_breaker_state VARCHAR(32) NOT NULL, -- CLOSED, HALF_OPEN, OPEN
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed Initial Exchanges
INSERT INTO exchanges (exchange_id, exchange_name, status, heartbeat_interval_ms) VALUES
('BINANCE_US', 'Binance US Global Liquidity Pool', 'OPERATIONAL', 200),
('COINBASE_PRO', 'Coinbase Institutional Prime Gateway', 'OPERATIONAL', 250),
('NASDAQ_ITCH', 'NASDAQ TotalView ITCH Direct Feed', 'OPERATIONAL', 100),
('KRAKEN_CORE', 'Kraken Multi-Asset Core Matcher', 'OPERATIONAL', 300);

-- Seed Initial Instruments
INSERT INTO instruments (symbol, asset_class, base_currency, quote_currency, tick_size, min_volume) VALUES
('BTC-USD', 'CRYPTO', 'BTC', 'USD', 0.50, 0.0001),
('ETH-USD', 'CRYPTO', 'ETH', 'USD', 0.05, 0.001),
('NVDA', 'EQUITIES', 'NVDA', 'USD', 0.01, 1.0),
('AAPL', 'EQUITIES', 'AAPL', 'USD', 0.01, 1.0),
('SOL-USD', 'CRYPTO', 'SOL', 'USD', 0.01, 0.01);

-- Seed Sample Anomaly Incidents
INSERT INTO anomaly_incidents (symbol, exchange_id, anomaly_type, severity, trigger_price, baseline_price, deviation_pct, trigger_volume, action_taken, raw_telemetry) VALUES
('BTC-USD', 'BINANCE_US', 'FLASH_CRASH', 'CRITICAL', 89200.0, 95500.0, -6.60, 480.5, 'QUARANTINED', '{"z_score": -4.2, "window_seconds": 10, "source": "rapid_dump"}'),
('NVDA', 'NASDAQ_ITCH', 'VOLATILITY_SPIKE', 'HIGH', 142.50, 134.20, 6.18, 1250000.0, 'ALERTED', '{"z_score": 3.8, "earnings_surge": true}'),
('ETH-USD', 'COINBASE_PRO', 'SPREAD_INVERSION', 'MEDIUM', 3350.0, 3350.0, 0.0, 55.0, 'ALERTED', '{"bid": 3352.0, "ask": 3349.0, "spread": -3.0}');

-- Seed Initial Ticks
INSERT INTO market_ticks (symbol, exchange_id, price, volume, bid, ask, vwap, z_score) VALUES
('BTC-USD', 'BINANCE_US', 95500.0, 2.5, 95498.0, 95502.0, 95490.0, 0.15),
('ETH-USD', 'COINBASE_PRO', 3350.0, 14.2, 3349.5, 3350.5, 3348.0, -0.22),
('NVDA', 'NASDAQ_ITCH', 135.20, 5000.0, 135.15, 135.25, 135.10, 0.45);

-- Seed Sample Dropout Events
INSERT INTO exchange_dropout_events (exchange_id, failure_type, missed_heartbeats, duration_ms, circuit_breaker_state) VALUES
('KRAKEN_CORE', 'MISSED_HEARTBEATS', 4, 1200, 'OPEN');

-- Synchronize sequences
SELECT setval('market_ticks_tick_id_seq', (SELECT COALESCE(MAX(tick_id), 1) FROM market_ticks));
SELECT setval('anomaly_incidents_incident_id_seq', (SELECT COALESCE(MAX(incident_id), 1) FROM anomaly_incidents));
SELECT setval('exchange_dropout_events_dropout_id_seq', (SELECT COALESCE(MAX(dropout_id), 1) FROM exchange_dropout_events));
