#include "marketsentinel.h"
#include <cmath>
#include <cstring>
#include <cstdio>
#include <chrono>
#include <algorithm>

extern "C" {

void evaluate_market_tick(const CMarketTick* tick, const CBaselineMetrics* baseline, CAnomalyResult* out_result) {
    if (!tick || !baseline || !out_result) return;

    auto t_start = std::chrono::high_resolution_clock::now();

    out_result->is_anomaly = 0;
    out_result->anomaly_type = 0;
    out_result->z_score = 0.0;
    out_result->computed_vwap = baseline->rolling_vwap;
    out_result->deviation_pct = 0.0;
    std::strcpy(out_result->alert_reason, "NORMAL_TICK");

    // 1. Check Spread Inversion (Bid > Ask or Negative Spread)
    if (tick->bid > 0.0 && tick->ask > 0.0 && tick->bid >= tick->ask) {
        out_result->is_anomaly = 1;
        out_result->anomaly_type = 3; // SPREAD_INVERSION
        out_result->deviation_pct = ((tick->bid - tick->ask) / tick->ask) * 100.0;
        std::snprintf(out_result->alert_reason, sizeof(out_result->alert_reason),
                      "Spread inversion: Bid %.2f >= Ask %.2f on %s", tick->bid, tick->ask, tick->exchange_id);
        auto t_end = std::chrono::high_resolution_clock::now();
        out_result->latency_ns = (int32_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t_end - t_start).count();
        return;
    }

    // 2. Compute Z-Score against Rolling Baseline Mean & StdDev
    double std_dev = (baseline->rolling_std_dev > 1e-6) ? baseline->rolling_std_dev : 1.0;
    double z = (tick->price - baseline->rolling_mean) / std_dev;
    out_result->z_score = z;

    double dev_pct = 0.0;
    if (baseline->rolling_mean > 1e-6) {
        dev_pct = ((tick->price - baseline->rolling_mean) / baseline->rolling_mean) * 100.0;
    }
    out_result->deviation_pct = dev_pct;

    // 3. Detect Flash Crash: Z < -3.0 or sudden drop > 4.5%
    if (z < -3.0 || dev_pct < -4.5) {
        out_result->is_anomaly = 1;
        out_result->anomaly_type = 1; // FLASH_CRASH
        std::snprintf(out_result->alert_reason, sizeof(out_result->alert_reason),
                      "CRITICAL Flash Crash: Price %.2f plunged %.2f%% (Z=%.2f)", tick->price, dev_pct, z);
        auto t_end = std::chrono::high_resolution_clock::now();
        out_result->latency_ns = (int32_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t_end - t_start).count();
        return;
    }

    // 4. Detect Pump and Dump: Z > 3.5 accompanied by massive volume surge (> 3.5x average)
    if (z > 3.5 && tick->volume > (baseline->avg_volume * 3.5) && baseline->avg_volume > 0.0) {
        out_result->is_anomaly = 1;
        out_result->anomaly_type = 2; // PUMP_DUMP
        std::snprintf(out_result->alert_reason, sizeof(out_result->alert_reason),
                      "CRITICAL Pump & Dump: Price spiked %.2f%% with %.1fx volume surge", dev_pct, tick->volume / baseline->avg_volume);
        auto t_end = std::chrono::high_resolution_clock::now();
        out_result->latency_ns = (int32_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t_end - t_start).count();
        return;
    }

    // 5. Volume Spike Anomaly
    if (tick->volume > (baseline->avg_volume * 8.0) && baseline->avg_volume > 0.0) {
        out_result->is_anomaly = 1;
        out_result->anomaly_type = 5; // VOLUME_SPIKE
        std::snprintf(out_result->alert_reason, sizeof(out_result->alert_reason),
                      "Abnormal Volume Spike: %.2f (%.1fx avg)", tick->volume, tick->volume / baseline->avg_volume);
        auto t_end = std::chrono::high_resolution_clock::now();
        out_result->latency_ns = (int32_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t_end - t_start).count();
        return;
    }

    auto t_end = std::chrono::high_resolution_clock::now();
    out_result->latency_ns = (int32_t)std::chrono::duration_cast<std::chrono::nanoseconds>(t_end - t_start).count();
}

void batch_evaluate_ticks(const CMarketTick* ticks, int32_t count, const CBaselineMetrics* baselines, CAnomalyResult* out_results) {
    if (!ticks || !baselines || !out_results || count <= 0) return;

    // SIMD loop unrolling for high pipeline throughput
    int32_t i = 0;
    for (; i <= count - 4; i += 4) {
        evaluate_market_tick(&ticks[i], &baselines[i], &out_results[i]);
        evaluate_market_tick(&ticks[i+1], &baselines[i+1], &out_results[i+1]);
        evaluate_market_tick(&ticks[i+2], &baselines[i+2], &out_results[i+2]);
        evaluate_market_tick(&ticks[i+3], &baselines[i+3], &out_results[i+3]);
    }
    for (; i < count; ++i) {
        evaluate_market_tick(&ticks[i], &baselines[i], &out_results[i]);
    }
}

void calculate_rolling_vwap_zscore(const double* prices, const double* volumes, int32_t count, double* out_vwap, double* out_zscore) {
    if (!prices || !volumes || count <= 0 || !out_vwap || !out_zscore) return;

    double sum_pv = 0.0;
    double sum_v = 0.0;
    double sum_p = 0.0;
    double sum_p_sq = 0.0;

    for (int32_t i = 0; i < count; ++i) {
        double p = prices[i];
        double v = volumes[i];
        sum_pv += (p * v);
        sum_v += v;
        sum_p += p;
        sum_p_sq += (p * p);
    }

    *out_vwap = (sum_v > 0.0) ? (sum_pv / sum_v) : prices[count - 1];

    double mean = sum_p / count;
    double variance = (sum_p_sq / count) - (mean * mean);
    double std_dev = (variance > 1e-6) ? std::sqrt(variance) : 1.0;

    double latest_price = prices[count - 1];
    *out_zscore = (latest_price - mean) / std_dev;
}

}
