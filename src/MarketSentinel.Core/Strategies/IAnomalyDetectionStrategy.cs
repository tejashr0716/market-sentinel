using MarketSentinel.Core.Models;

namespace MarketSentinel.Core.Strategies;

public interface IAnomalyDetectionStrategy
{
    AnomalyResult Evaluate(in MarketTick tick, in BaselineMetrics baseline);
}

public class FlashCrashStrategy : IAnomalyDetectionStrategy
{
    private const double CriticalZThreshold = -3.0;
    private const double MaxPricePlungePercent = -4.5;

    public AnomalyResult Evaluate(in MarketTick tick, in BaselineMetrics baseline)
    {
        if (baseline.RollingStdDev <= 1e-6 || baseline.RollingMean <= 1e-6)
            return new AnomalyResult(false, AnomalyType.None, AnomalySeverity.Low, 0, 0, "INSUFFICIENT_BASELINE", 0);

        double z = (tick.Price - baseline.RollingMean) / baseline.RollingStdDev;
        double devPct = ((tick.Price - baseline.RollingMean) / baseline.RollingMean) * 100.0;

        if (z < CriticalZThreshold || devPct < MaxPricePlungePercent)
        {
            return new AnomalyResult(
                true,
                AnomalyType.FlashCrash,
                AnomalySeverity.Critical,
                z,
                devPct,
                $"Flash Crash Alert: Price {tick.Price:F2} dropped {devPct:F2}% (Z={z:F2})",
                42
            );
        }

        return new AnomalyResult(false, AnomalyType.None, AnomalySeverity.Low, z, devPct, "NORMAL", 0);
    }
}

public class PumpDumpStrategy : IAnomalyDetectionStrategy
{
    private const double CriticalZSpike = 3.5;
    private const double VolumeMultiplier = 3.5;

    public AnomalyResult Evaluate(in MarketTick tick, in BaselineMetrics baseline)
    {
        if (baseline.RollingStdDev <= 1e-6 || baseline.AvgVolume <= 1e-6)
            return new AnomalyResult(false, AnomalyType.None, AnomalySeverity.Low, 0, 0, "INSUFFICIENT_BASELINE", 0);

        double z = (tick.Price - baseline.RollingMean) / baseline.RollingStdDev;
        double devPct = ((tick.Price - baseline.RollingMean) / baseline.RollingMean) * 100.0;

        if (z > CriticalZSpike && tick.Volume > (baseline.AvgVolume * VolumeMultiplier))
        {
            return new AnomalyResult(
                true,
                AnomalyType.PumpAndDump,
                AnomalySeverity.Critical,
                z,
                devPct,
                $"Pump & Dump Surge: Price spiked {devPct:F2}% with {tick.Volume / baseline.AvgVolume:F1}x volume surge",
                55
            );
        }

        return new AnomalyResult(false, AnomalyType.None, AnomalySeverity.Low, z, devPct, "NORMAL", 0);
    }
}

public class SpreadInversionStrategy : IAnomalyDetectionStrategy
{
    public AnomalyResult Evaluate(in MarketTick tick, in BaselineMetrics baseline)
    {
        if (tick.Bid > 0 && tick.Ask > 0 && tick.Bid >= tick.Ask)
        {
            double spreadDev = ((tick.Bid - tick.Ask) / tick.Ask) * 100.0;
            return new AnomalyResult(
                true,
                AnomalyType.SpreadInversion,
                AnomalySeverity.Medium,
                0,
                spreadDev,
                $"Crossed Book Inversion: Bid {tick.Bid:F2} >= Ask {tick.Ask:F2} on {tick.ExchangeId}",
                30
            );
        }

        return new AnomalyResult(false, AnomalyType.None, AnomalySeverity.Low, 0, 0, "NORMAL", 0);
    }
}
