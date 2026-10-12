"""Distributed dataset inspection (Spark).

Profiles the raw AIS CSV without loading it into pandas:
schema, sample rows, row/vessel counts, per-column null counts, numeric
ranges, timestamp-format validity and categorical distributions.

Usage::

    python -m src.ingestion.inspect_dataset            # full profile
    python -m src.ingestion.inspect_dataset --sample 0.01

The JSON report is written to output/reports/dataset_profile.json.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pyspark.sql import functions as F

from src.common.config import PROJECT_ROOT
from src.common.paths import raw_input
from src.common.schemas import RAW_AIS_SCHEMA
from src.common.spark import get_spark

NUMERIC = ["longitude", "latitude", "sog", "cog", "heading", "length", "width", "draft"]
TS_REGEX = r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"


def profile(sample: float | None) -> dict:
    spark = get_spark("inspect_dataset")
    t0 = time.time()
    df = (spark.read.option("header", "true").schema(RAW_AIS_SCHEMA).csv(raw_input()))
    if sample:
        df = df.sample(fraction=sample, seed=7)

    print("== Schema ==")
    df.printSchema()
    print("== Sample rows ==")
    df.show(5, truncate=False)

    # Single aggregation pass for counts, nulls (empty strings count as null
    # for text columns) and numeric ranges.
    aggs = [F.count(F.lit(1)).alias("rows"), F.countDistinct("mmsi").alias("distinct_mmsi")]
    for c, t in [(f.name, f.dataType.simpleString()) for f in RAW_AIS_SCHEMA.fields]:
        missing = F.col(c).isNull() | (F.trim(F.col(c)) == "") if t == "string" else F.col(c).isNull()
        aggs.append(F.sum(F.when(missing, 1).otherwise(0)).alias(f"null__{c}"))
    for c in NUMERIC:
        aggs += [F.min(c).alias(f"min__{c}"), F.max(c).alias(f"max__{c}")]
    aggs += [
        F.min("base_date_time").alias("ts_min"), F.max("base_date_time").alias("ts_max"),
        F.sum(F.when(~F.col("base_date_time").rlike(TS_REGEX), 1).otherwise(0)).alias("bad_ts"),
        F.sum(F.when((F.abs("latitude") < 0.01) & (F.abs("longitude") < 0.01), 1).otherwise(0)).alias("null_island"),
        F.sum(F.when(~F.col("latitude").between(-90, 90) | ~F.col("longitude").between(-180, 180), 1)
              .otherwise(0)).alias("out_of_range_coords"),
    ]
    row = df.agg(*aggs).first().asDict()
    rows = row["rows"]

    def dist(col: str, n: int = 25):
        return [{"value": r[col], "count": r["count"]}
                for r in df.groupBy(col).count().orderBy(F.desc("count")).limit(n).collect()]

    dup = rows - df.select("mmsi", "base_date_time").distinct().count()

    report = {
        "source": raw_input(),
        "sample_fraction": sample,
        "rows": rows,
        "distinct_mmsi": row["distinct_mmsi"],
        "timestamp_range": [row["ts_min"], row["ts_max"]],
        "malformed_timestamps": row["bad_ts"],
        "duplicate_mmsi_timestamp_rows": dup,
        "null_island_rows": row["null_island"],
        "out_of_range_coordinate_rows": row["out_of_range_coords"],
        "missing": {c.name: {"count": row[f"null__{c.name}"],
                             "pct": round(100 * row[f"null__{c.name}"] / rows, 3)}
                    for c in RAW_AIS_SCHEMA.fields},
        "ranges": {c: [row[f"min__{c}"], row[f"max__{c}"]] for c in NUMERIC},
        "distributions": {"status": dist("status"), "transceiver": dist("transceiver"),
                          "vessel_type": dist("vessel_type")},
        "spark_master": spark.sparkContext.master,
        "default_parallelism": spark.sparkContext.defaultParallelism,
        "input_partitions": df.rdd.getNumPartitions(),
        "elapsed_seconds": round(time.time() - t0, 1),
    }
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=float, default=None, help="optional sample fraction")
    args = ap.parse_args()
    report = profile(args.sample)
    out = PROJECT_ROOT / "output" / "reports"
    out.mkdir(parents=True, exist_ok=True)
    (out / "dataset_profile.json").write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps({k: v for k, v in report.items() if k != "distributions"}, indent=2, default=str))


if __name__ == "__main__":
    main()
