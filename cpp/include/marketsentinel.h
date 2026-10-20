#ifndef MARKETSENTINEL_H
#define MARKETSENTINEL_H

#include <cstdint>
#include <cstddef>

#ifdef __cplusplus
extern "C" {
#endif

#pragma pack(push, 8)

typedef struct {
    char symbol[32];
    char exchange_id[32];
    double price;
    double volume;
    double bid;
    double ask;
    int64_t timestamp_ns;
    uint64_t sequence_no;
} CMarketTick;

typedef struct {
    double rolling_mean;
    double rolling_std_dev;
    double rolling_vwap;
    double avg_volume;
    int64_t last_heartbeat_ms;
    int32_t heartbeat_threshold_ms;
} CBaselineMetrics;

typedef struct {
    int32_t is_anomaly;       // 0: NORMAL, 1: ANOMALY_DETECTED, 2: SERVICE_DROPOUT
    int32_t anomaly_type;     // 0: NONE, 1: FLASH_CRASH, 2: PUMP_DUMP, 3: SPREAD_INVERSION, 4: HEARTBEAT_DROPOUT, 5: VOLUME_SPIKE
    double z_score;
    double computed_vwap;
    double deviation_pct;
    int32_t latency_ns;
    char alert_reason[128];
} CAnomalyResult;

#pragma pack(pop)

// Single Tick Evaluation API
void evaluate_market_tick(const CMarketTick* tick, const CBaselineMetrics* baseline, CAnomalyResult* out_result);

// Batch SIMD Vectorized Evaluation
void batch_evaluate_ticks(const CMarketTick* ticks, int32_t count, const CBaselineMetrics* baselines, CAnomalyResult* out_results);

// Fast Online Rolling Welford & VWAP Calculation
void calculate_rolling_vwap_zscore(const double* prices, const double* volumes, int32_t count, double* out_vwap, double* out_zscore);

#ifdef __cplusplus
}
#endif

#endif // MARKETSENTINEL_H
