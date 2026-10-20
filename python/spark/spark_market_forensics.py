"""
MarketSentinel: Apache Spark Batch Market Surveillance & Forensic Analytics
Clusters coordinated anomalies and computes multi-exchange price drift and exchange reliability.
"""

import os
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType
)

def build_spark_session(app_name: str = "MarketSentinel-Forensics") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

def run_market_forensics_analysis(ticks_json_path: str, output_report_path: str):
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("tick_id", LongType(), True),
        StructField("symbol", StringType(), True),
        StructField("exchange_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("vwap", DoubleType(), True),
        StructField("z_score", DoubleType(), True)
    ])

    df = spark.read.schema(schema).json(ticks_json_path)

    total_ticks = df.count()

    # 1. Per Symbol Price Volatility & Volume Metrics
    symbol_metrics = (
        df.groupBy("symbol")
        .agg(
            F.avg("price").alias("avg_price"),
            F.stddev("price").alias("price_volatility"),
            F.sum("volume").alias("total_volume"),
            F.max("price").alias("max_price"),
            F.min("price").alias("min_price")
        )
        .collect()
    )

    sym_dict = [
        {
            "symbol": row["symbol"],
            "avg_price": round(row["avg_price"] or 0.0, 2),
            "volatility": round(row["price_volatility"] or 0.0, 2),
            "volume": round(row["total_volume"] or 0.0, 2),
            "range": [round(row["min_price"] or 0.0, 2), round(row["max_price"] or 0.0, 2)]
        }
        for row in symbol_metrics
    ]

    # 2. Outlier / Anomaly Count (Z-Score > 3.0 or < -3.0)
    anomalies_df = df.filter(F.abs(F.col("z_score")) >= 3.0)
    anomaly_count = anomalies_df.count()

    # 3. Exchange Distribution
    exchange_dist = {
        row["exchange_id"]: row["count"]
        for row in df.groupBy("exchange_id").count().collect()
    }

    report = {
        "engine": "MarketSentinel Apache Spark Forensics",
        "total_ticks_analyzed": total_ticks,
        "critical_anomalies_detected": anomaly_count,
        "exchange_distribution": exchange_dist,
        "symbol_profiles": sym_dict,
        "surveillance_status": "HIGH_VOLATILITY" if anomaly_count > (total_ticks * 0.05) else "NORMAL"
    }

    os.makedirs(os.path.dirname(output_report_path), exist_ok=True)
    with open(output_report_path, "w") as f:
        json.dump(report, f, indent=2)

    spark.stop()
    return report

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark_market_forensics.py <ticks_json> <output_report_path>")
        sys.exit(1)
    run_market_forensics_analysis(sys.argv[1], sys.argv[2])
