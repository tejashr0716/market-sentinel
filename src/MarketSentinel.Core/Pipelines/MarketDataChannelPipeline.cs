using System.Threading.Channels;
using MarketSentinel.Core.Models;
using MarketSentinel.Core.Strategies;

namespace MarketSentinel.Core.Pipelines;

public class MarketDataChannelPipeline
{
    private readonly Channel<MarketTick> _incomingChannel;
    private readonly List<IAnomalyDetectionStrategy> _strategies;
    private long _processedTicksCount;

    public MarketDataChannelPipeline(int capacity = 50000)
    {
        var options = new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = false
        };
        _incomingChannel = Channel.CreateBounded<MarketTick>(options);
        _strategies = new List<IAnomalyDetectionStrategy>
        {
            new SpreadInversionStrategy(),
            new FlashCrashStrategy(),
            new PumpDumpStrategy()
        };
    }

    public ChannelWriter<MarketTick> Writer => _incomingChannel.Writer;
    public ChannelReader<MarketTick> Reader => _incomingChannel.Reader;
    public long ProcessedTicksCount => Interlocked.Read(ref _processedTicksCount);

    public async ValueTask<bool> PublishTickAsync(MarketTick tick, CancellationToken ct = default)
    {
        return await _incomingChannel.Writer.WaitToWriteAsync(ct) && _incomingChannel.Writer.TryWrite(tick);
    }

    public AnomalyResult EvaluateTick(in MarketTick tick, in BaselineMetrics baseline)
    {
        Interlocked.Increment(ref _processedTicksCount);
        foreach (var strategy in _strategies)
        {
            var res = strategy.Evaluate(tick, baseline);
            if (res.IsAnomaly)
            {
                return res;
            }
        }
        return new AnomalyResult(false, AnomalyType.None, AnomalySeverity.Low, 0, 0, "NORMAL_FLOW", 15);
    }
}
