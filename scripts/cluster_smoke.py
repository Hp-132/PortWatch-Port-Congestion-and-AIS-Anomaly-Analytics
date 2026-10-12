"""Cluster smoke test: prove a Spark job really runs distributed over HDFS.

Reads the raw AIS file from HDFS, aggregates it, runs a Python function on the
executors, and prints which executors (YARN containers / standalone workers)
did the work, from the Spark status REST API.

    . .\\scripts\\env_cluster.ps1
    python scripts/cluster_smoke.py                       # YARN (MX_SPARK_MASTER=yarn)
    $env:MX_SPARK_MASTER="spark://localhost:7077"; python scripts/cluster_smoke.py
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F  # noqa: E402

from src.common.paths import raw_input  # noqa: E402
from src.common.spark import get_spark  # noqa: E402


def main():
    t0 = time.time()
    spark = get_spark("cluster smoke test")
    sc = spark.sparkContext
    print(f"master={sc.master} app_id={sc.applicationId} ui={sc.uiWebUrl}")
    df = spark.read.option("header", "true").csv(raw_input())
    print(f"input={raw_input()} partitions={df.rdd.getNumPartitions()}")
    rows = df.count()
    vessels = df.select("mmsi").distinct().count()
    per_day = (df.groupBy(F.substring("base_date_time", 1, 10).alias("day")).count().orderBy("day").collect())
    print(f"rows={rows:,} distinct_mmsi={vessels:,}")
    for r in per_day:
        print(f"  {r['day']}  {r['count']:,}")

    # Python on the executors: which process/host handled each partition
    def where(it):
        import os as _os
        import socket
        n = sum(1 for _ in it)
        yield (socket.gethostname(), _os.getpid(), n)
    placed = df.limit(2_000_000).repartition(12).rdd.mapPartitions(where).collect()
    print(f"python workers used: {len({p[1] for p in placed})} processes")

    with urllib.request.urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/executors") as resp:
        execs = json.load(resp)
    for e in execs:
        print(f"  executor {e['id']:>6} host={e['hostPort']:<34} cores={e['totalCores']} "
              f"tasks_completed={e['completedTasks']} input={e['totalInputBytes'] / 1e9:.2f} GB")
    print(f"elapsed {time.time() - t0:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
