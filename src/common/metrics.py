"""Pipeline metrics: small key/value records written to the lake per stage.

They are produced by Spark jobs (real computed numbers, never constants) and
later loaded into MongoDB for the dashboard's data-quality / pipeline panel.
"""
from __future__ import annotations

import json
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from .paths import lake_path

_SCHEMA = StructType([StructField("metric", StringType()), StructField("value", StringType())])


def record_metrics(spark: SparkSession, stage: str, metrics: dict) -> None:
    rows = [(k, json.dumps(v, default=str)) for k, v in metrics.items()]
    rows.append(("recorded_at", json.dumps(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))))
    rows.append(("spark_master", json.dumps(spark.sparkContext.master)))
    (spark.createDataFrame(rows, _SCHEMA).coalesce(1).write.mode("overwrite")
     .option("header", "true").option("escape", '"').csv(f"{lake_path('pipeline_metrics')}/stage={stage}"))
    print(f"[metrics:{stage}] " + ", ".join(f"{k}={v}" for k, v in metrics.items()))


def read_metrics(spark: SparkSession, stage: str) -> dict:
    df = (spark.read.option("header", "true").option("escape", '"')
          .csv(f"{lake_path('pipeline_metrics')}/stage={stage}"))
    return {r["metric"]: json.loads(r["value"]) for r in df.collect()}


class Timer:
    def __enter__(self):
        self.t0 = time.time()
        return self

    def __exit__(self, *exc):
        self.seconds = round(time.time() - self.t0, 1)
