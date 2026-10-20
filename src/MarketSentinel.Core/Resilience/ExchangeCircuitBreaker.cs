namespace MarketSentinel.Core.Resilience;

public enum CircuitState
{
    Closed,
    HalfOpen,
    Open
}

public class ExchangeCircuitBreaker
{
    private readonly int _failureThreshold;
    private readonly TimeSpan _recoveryTimeout;
    private int _consecutiveMissedHeartbeats;
    private long _lastStateChangeTicks;
    private int _currentState; // 0: Closed, 1: HalfOpen, 2: Open

    public ExchangeCircuitBreaker(int failureThreshold = 3, TimeSpan? recoveryTimeout = null)
    {
        _failureThreshold = failureThreshold;
        _recoveryTimeout = recoveryTimeout ?? TimeSpan.FromMilliseconds(1500);
        _currentState = (int)CircuitState.Closed;
        _lastStateChangeTicks = DateTime.UtcNow.Ticks;
    }

    public CircuitState State => (CircuitState)_currentState;
    public int MissedHeartbeats => _consecutiveMissedHeartbeats;

    public bool RecordHeartbeatSuccess()
    {
        Interlocked.Exchange(ref _consecutiveMissedHeartbeats, 0);
        if (_currentState != (int)CircuitState.Closed)
        {
            Interlocked.Exchange(ref _currentState, (int)CircuitState.Closed);
            Interlocked.Exchange(ref _lastStateChangeTicks, DateTime.UtcNow.Ticks);
            return true; // Circuit restored
        }
        return false;
    }

    public CircuitState RecordHeartbeatMissed()
    {
        int missed = Interlocked.Increment(ref _consecutiveMissedHeartbeats);
        if (missed >= _failureThreshold && _currentState == (int)CircuitState.Closed)
        {
            Interlocked.Exchange(ref _currentState, (int)CircuitState.Open);
            Interlocked.Exchange(ref _lastStateChangeTicks, DateTime.UtcNow.Ticks);
        }
        return (CircuitState)_currentState;
    }

    public bool AllowExecution()
    {
        int state = _currentState;
        if (state == (int)CircuitState.Closed) return true;

        if (state == (int)CircuitState.Open)
        {
            long elapsedTicks = DateTime.UtcNow.Ticks - Interlocked.Read(ref _lastStateChangeTicks);
            if (TimeSpan.FromTicks(elapsedTicks) > _recoveryTimeout)
            {
                Interlocked.CompareExchange(ref _currentState, (int)CircuitState.HalfOpen, (int)CircuitState.Open);
                return true; // Trial request
            }
            return false; // Trip calls
        }

        return true; // HalfOpen trial
    }
}
