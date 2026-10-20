import ctypes
import os
from typing import List, Dict, Any, Optional

class CMarketTick(ctypes.Structure):
    _layout_ = "ms"
    _pack_ = 8
    _fields_ = [
        ("symbol", ctypes.c_char * 32),
        ("exchange_id", ctypes.c_char * 32),
        ("price", ctypes.c_double),
        ("volume", ctypes.c_double),
        ("bid", ctypes.c_double),
        ("ask", ctypes.c_double),
        ("timestamp_ns", ctypes.c_int64),
        ("sequence_no", ctypes.c_uint64)
    ]

class CBaselineMetrics(ctypes.Structure):
    _layout_ = "ms"
    _pack_ = 8
    _fields_ = [
        ("rolling_mean", ctypes.c_double),
        ("rolling_std_dev", ctypes.c_double),
        ("rolling_vwap", ctypes.c_double),
        ("avg_volume", ctypes.c_double),
        ("last_heartbeat_ms", ctypes.c_int64),
        ("heartbeat_threshold_ms", ctypes.c_int32)
    ]

class CAnomalyResult(ctypes.Structure):
    _layout_ = "ms"
    _pack_ = 8
    _fields_ = [
        ("is_anomaly", ctypes.c_int32),
        ("anomaly_type", ctypes.c_int32),
        ("z_score", ctypes.c_double),
        ("computed_vwap", ctypes.c_double),
        ("deviation_pct", ctypes.c_double),
        ("latency_ns", ctypes.c_int32),
        ("alert_reason", ctypes.c_char * 128)
    ]

ANOMALY_TYPE_NAMES = {
    0: "NONE",
    1: "FLASH_CRASH",
    2: "PUMP_AND_DUMP",
    3: "SPREAD_INVERSION",
    4: "HEARTBEAT_DROPOUT",
    5: "VOLUME_SPIKE"
}

class MarketSentinelEngine:
    def __init__(self, dylib_path: Optional[str] = None):
        if dylib_path is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            dylib_path = os.path.join(base_dir, "libmarketsentinel.dylib")

        if not os.path.exists(dylib_path):
            raise FileNotFoundError(f"Shared library not found at: {dylib_path}")

        self.lib = ctypes.CDLL(dylib_path)

        # void evaluate_market_tick(...)
        self.lib.evaluate_market_tick.argtypes = [
            ctypes.POINTER(CMarketTick),
            ctypes.POINTER(CBaselineMetrics),
            ctypes.POINTER(CAnomalyResult)
        ]
        self.lib.evaluate_market_tick.restype = None

        # void batch_evaluate_ticks(...)
        self.lib.batch_evaluate_ticks.argtypes = [
            ctypes.POINTER(CMarketTick),
            ctypes.c_int32,
            ctypes.POINTER(CBaselineMetrics),
            ctypes.POINTER(CAnomalyResult)
        ]
        self.lib.batch_evaluate_ticks.restype = None

        # void calculate_rolling_vwap_zscore(...)
        self.lib.calculate_rolling_vwap_zscore.argtypes = [
            ctypes.POINTER(ctypes.c_double),
            ctypes.POINTER(ctypes.c_double),
            ctypes.c_int32,
            ctypes.POINTER(ctypes.c_double),
            ctypes.POINTER(ctypes.c_double)
        ]
        self.lib.calculate_rolling_vwap_zscore.restype = None

    def evaluate_tick(
        self,
        symbol: str,
        exchange_id: str,
        price: float,
        volume: float,
        bid: float,
        ask: float,
        baseline_mean: float,
        baseline_std: float,
        baseline_vwap: float = 0.0,
        avg_volume: float = 1.0,
        timestamp_ns: int = 0,
        seq_no: int = 0
    ) -> Dict[str, Any]:
        tick = CMarketTick(
            symbol=symbol.encode("utf-8"),
            exchange_id=exchange_id.encode("utf-8"),
            price=price,
            volume=volume,
            bid=bid,
            ask=ask,
            timestamp_ns=timestamp_ns,
            sequence_no=seq_no
        )
        base = CBaselineMetrics(
            rolling_mean=baseline_mean,
            rolling_std_dev=baseline_std,
            rolling_vwap=baseline_vwap,
            avg_volume=avg_volume,
            last_heartbeat_ms=0,
            heartbeat_threshold_ms=500
        )
        res = CAnomalyResult()
        self.lib.evaluate_market_tick(ctypes.byref(tick), ctypes.byref(base), ctypes.byref(res))

        return {
            "is_anomaly": bool(res.is_anomaly),
            "anomaly_type_code": res.anomaly_type,
            "anomaly_type": ANOMALY_TYPE_NAMES.get(res.anomaly_type, "UNKNOWN"),
            "z_score": round(res.z_score, 2),
            "computed_vwap": round(res.computed_vwap, 2),
            "deviation_pct": round(res.deviation_pct, 2),
            "latency_ns": res.latency_ns,
            "reason": res.alert_reason.decode("utf-8")
        }

    def batch_evaluate(
        self,
        ticks_list: List[Dict[str, Any]],
        baselines_list: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        n = len(ticks_list)
        if n == 0:
            return []

        c_ticks = (CMarketTick * n)()
        c_bases = (CBaselineMetrics * n)()
        c_res = (CAnomalyResult * n)()

        for i in range(n):
            t = ticks_list[i]
            b = baselines_list[i]
            c_ticks[i] = CMarketTick(
                symbol=str(t.get("symbol", "")).encode("utf-8"),
                exchange_id=str(t.get("exchange_id", "")).encode("utf-8"),
                price=float(t.get("price", 0.0)),
                volume=float(t.get("volume", 0.0)),
                bid=float(t.get("bid", 0.0)),
                ask=float(t.get("ask", 0.0)),
                timestamp_ns=int(t.get("timestamp_ns", 0)),
                sequence_no=int(t.get("sequence_no", 0))
            )
            c_bases[i] = CBaselineMetrics(
                rolling_mean=float(b.get("rolling_mean", 0.0)),
                rolling_std_dev=float(b.get("rolling_std_dev", 1.0)),
                rolling_vwap=float(b.get("rolling_vwap", 0.0)),
                avg_volume=float(b.get("avg_volume", 1.0)),
                last_heartbeat_ms=0,
                heartbeat_threshold_ms=500
            )

        self.lib.batch_evaluate_ticks(c_ticks, n, c_bases, c_res)

        output = []
        for i in range(n):
            output.append({
                "is_anomaly": bool(c_res[i].is_anomaly),
                "anomaly_type": ANOMALY_TYPE_NAMES.get(c_res[i].anomaly_type, "UNKNOWN"),
                "z_score": round(c_res[i].z_score, 2),
                "deviation_pct": round(c_res[i].deviation_pct, 2),
                "latency_ns": c_res[i].latency_ns,
                "reason": c_res[i].alert_reason.decode("utf-8")
            })
        return output

    def compute_vwap_and_zscore(self, prices: List[float], volumes: List[float]) -> Dict[str, float]:
        n = len(prices)
        if n == 0:
            return {"vwap": 0.0, "z_score": 0.0}

        p_arr = (ctypes.c_double * n)(*prices)
        v_arr = (ctypes.c_double * n)(*volumes)
        vwap = ctypes.c_double()
        z_score = ctypes.c_double()

        self.lib.calculate_rolling_vwap_zscore(p_arr, v_arr, n, ctypes.byref(vwap), ctypes.byref(z_score))
        return {
            "vwap": round(vwap.value, 2),
            "z_score": round(z_score.value, 2)
        }
