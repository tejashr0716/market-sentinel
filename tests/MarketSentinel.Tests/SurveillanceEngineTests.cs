using MarketSentinel.Core.Models;
using MarketSentinel.Core.Pipelines;
using MarketSentinel.Core.Resilience;
using MarketSentinel.Core.Strategies;
using Xunit;
using FluentAssertions;

namespace MarketSentinel.Tests;

public class SurveillanceEngineTests
{
    [Fact]
    public void FlashCrashStrategy_ShouldTriggerCriticalAnomaly_WhenPriceDropsSharply()
    {
        var strategy = new FlashCrashStrategy();
        var baseline = new BaselineMetrics(95000.0, 500.0, 94900.0, 2.0, 1700000000, 500);
        var tick = new MarketTick("BTC-USD", "BINANCE_US", 89500.0, 15.0, 89490.0, 89510.0, 1700000005, 101);

        var result = strategy.Evaluate(tick, baseline);

        result.IsAnomaly.Should().BeTrue();
        result.Type.Should().Be(AnomalyType.FlashCrash);
        result.Severity.Should().Be(AnomalySeverity.Critical);
        result.ZScore.Should().BeLessThan(-3.0);
    }

    [Fact]
    public void PumpDumpStrategy_ShouldTriggerAnomaly_WhenPriceAndVolumeSpikeConcurrently()
    {
        var strategy = new PumpDumpStrategy();
        var baseline = new BaselineMetrics(3000.0, 20.0, 3000.0, 10.0, 1700000000, 500);
        var tick = new MarketTick("ETH-USD", "COINBASE_PRO", 3150.0, 65.0, 3148.0, 3152.0, 1700000010, 102);

        var result = strategy.Evaluate(tick, baseline);

        result.IsAnomaly.Should().BeTrue();
        result.Type.Should().Be(AnomalyType.PumpAndDump);
        result.Severity.Should().Be(AnomalySeverity.Critical);
        result.ZScore.Should().BeGreaterThan(3.5);
    }

    [Fact]
    public void SpreadInversionStrategy_ShouldTriggerAnomaly_WhenBidExceedsAsk()
    {
        var strategy = new SpreadInversionStrategy();
        var baseline = new BaselineMetrics(150.0, 2.0, 150.0, 100.0, 1700000000, 500);
        var tick = new MarketTick("NVDA", "NASDAQ_ITCH", 150.0, 50.0, 151.20, 149.80, 1700000015, 103);

        var result = strategy.Evaluate(tick, baseline);

        result.IsAnomaly.Should().BeTrue();
        result.Type.Should().Be(AnomalyType.SpreadInversion);
        result.Reason.Should().Contain("Crossed Book Inversion");
    }

    [Fact]
    public void CircuitBreaker_ShouldTripOpen_AfterThresholdMissedHeartbeats()
    {
        var cb = new ExchangeCircuitBreaker(failureThreshold: 3);
        cb.State.Should().Be(CircuitState.Closed);

        cb.RecordHeartbeatMissed().Should().Be(CircuitState.Closed);
        cb.RecordHeartbeatMissed().Should().Be(CircuitState.Closed);
        cb.RecordHeartbeatMissed().Should().Be(CircuitState.Open);

        cb.AllowExecution().Should().BeFalse();

        // Successful heartbeat resets circuit
        cb.RecordHeartbeatSuccess().Should().BeTrue();
        cb.State.Should().Be(CircuitState.Closed);
        cb.AllowExecution().Should().BeTrue();
    }

    [Fact]
    public async Task ChannelPipeline_ShouldProcessTicksAsynchronously_WithoutDeadlocks()
    {
        var pipeline = new MarketDataChannelPipeline(capacity: 1000);
        var baseline = new BaselineMetrics(100.0, 2.0, 100.0, 10.0, 1700000000, 500);

        for (int i = 0; i < 100; i++)
        {
            var tick = new MarketTick("AAPL", "NASDAQ_ITCH", 100.0 + (i * 0.05), 5.0, 99.9, 100.1, 1700000000 + i, (ulong)i);
            await pipeline.PublishTickAsync(tick);
            var eval = pipeline.EvaluateTick(tick, baseline);
            eval.IsAnomaly.Should().BeFalse();
        }

        pipeline.ProcessedTicksCount.Should().Be(100);
    }
}
