namespace MarketSentinel.Core.Models;

public readonly record struct MarketTick(
    string Symbol,
    string ExchangeId,
    double Price,
    double Volume,
    double Bid,
    double Ask,
    long TimestampNs,
    ulong SequenceNo
);

public readonly record struct BaselineMetrics(
    double RollingMean,
    double RollingStdDev,
    double RollingVwap,
    double AvgVolume,
    long LastHeartbeatMs,
    int HeartbeatThresholdMs
);

public enum AnomalyType
{
    None = 0,
    FlashCrash = 1,
    PumpAndDump = 2,
    SpreadInversion = 3,
    HeartbeatDropout = 4,
    VolumeSpike = 5
}

public enum AnomalySeverity
{
    Low,
    Medium,
    High,
    Critical
}

public readonly record struct AnomalyResult(
    bool IsAnomaly,
    AnomalyType Type,
    AnomalySeverity Severity,
    double ZScore,
    double DeviationPct,
    string Reason,
    long LatencyNs
);
