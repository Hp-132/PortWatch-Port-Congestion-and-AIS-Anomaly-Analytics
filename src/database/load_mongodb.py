"""Stage 11 - Load analytical results into MongoDB (serving layer).

MongoDB only stores *results* (port metrics, visits, risk-scored gaps,
vessel summaries, map-ready positions/tracks) - never the raw 48M fixes,
which stay in HDFS / the lake.

Writes are distributed: each Spark partition is inserted by the executor
that holds it (``foreachPartition`` + pymongo bulk ``insert_many``). The
driver only drops/creates collections and indexes.

Usage::  python -m src.database.load_mongodb [--only ports,anomalies]
"""
from __future__ import annotations

import argparse
import json

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampNTZType, TimestampType

from src.common.config import load_config
from src.common.metrics import Timer
from src.common.paths import lake_path
from src.common.schemas import NAV_STATUS
from src.common.spark import get_spark, read_csv

BATCH = 2000
NEIGHBOURS_SCHEMA = ("array<struct<min_dist_nm:double,nb_mmsi:long,nb_group:string,side_start:int,side_end:int,"
                     "closest_epoch:long,closest_lat:double,closest_lon:double,reports_during:long,"
                     "reports_near_during:long,timeline:array<long>>>")


def _geo(lon: str, lat: str) -> F.Column:
    return F.when(F.col(lon).isNotNull() & F.col(lat).isNotNull(),
                  F.struct(F.lit("Point").alias("type"), F.array(F.col(lon), F.col(lat)).alias("coordinates")))


def _iso_timestamps(df: DataFrame) -> DataFrame:
    """Timestamps -> ISO-8601 UTC strings (avoids driver-local timezone drift).

    Dates -> 'yyyy-MM-dd' strings: BSON cannot encode ``datetime.date``, and on
    Windows the resulting InvalidDocument kills the Python worker without a
    traceback ("Python worker exited unexpectedly").
    """
    for f in df.schema.fields:
        if isinstance(f.dataType, (TimestampType, TimestampNTZType)):
            df = df.withColumn(f.name, F.date_format(F.col(f.name), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        elif isinstance(f.dataType, DateType):
            df = df.withColumn(f.name, F.date_format(F.col(f.name), "yyyy-MM-dd"))
    return df


def write_collection(df: DataFrame, collection: str, indexes: list, uri: str, db: str) -> int:
    from pymongo import MongoClient

    client = MongoClient(uri)
    client[db][collection].drop()
    df = _iso_timestamps(df)

    def _write_partition(rows):
        from pymongo import MongoClient as _MC
        c = _MC(uri)
        coll = c[db][collection]
        buf = []
        for r in rows:
            d = {k: v for k, v in r.asDict(recursive=True).items() if v is not None}
            buf.append(d)
            if len(buf) >= BATCH:
                coll.insert_many(buf, ordered=False)
                buf = []
        if buf:
            coll.insert_many(buf, ordered=False)
        c.close()

    df.foreachPartition(_write_partition)
    coll = client[db][collection]
    for idx in indexes:
        coll.create_index(idx)
    n = coll.estimated_document_count()
    client.close()
    print(f"[mongo] {collection:24s} {n:>10,d} documents")
    return n


def build_jobs(spark):
    """collection -> (DataFrame builder, indexes). Builders are lazy."""
    status_map = F.create_map(*[x for k, v in NAV_STATUS.items() for x in (F.lit(k), F.lit(v))])
    return {
        "ports": (lambda: read_csv(spark, "port_summary")
                  .withColumn("location", _geo("port_lon", "port_lat")),
                  [[("port_id", 1)], [("location", "2dsphere")], [("pci", -1)]]),
        "congestion_hourly": (lambda: read_csv(spark, "congestion_hourly"),
                              [[("port_id", 1), ("hour_epoch", 1)], [("hour_epoch", 1)]]),
        "congestion_daily": (lambda: read_csv(spark, "congestion_daily"),
                             [[("port_id", 1), ("date", 1)], [("date", 1)]]),
        "port_visits": (lambda: read_csv(spark, "turnaround_visits"),
                        [[("port_id", 1), ("arrival_epoch", 1)], [("mmsi", 1)], [("vessel_group", 1)]]),
        "turnaround_stats": (lambda: read_csv(spark, "turnaround_stats"),
                             [[("port_id", 1), ("vessel_group", 1)]]),
        "vessels": (lambda: read_csv(spark, "vessel_summary")
                    .withColumn("status_text", status_map[F.col("last_status")])
                    .withColumn("ports_visited", F.when(F.col("ports_visited").isNotNull(),
                                                        F.split("ports_visited", r"\|")))
                    .withColumn("location", _geo("last_lon", "last_lat")),
                    [[("mmsi", 1)], [("vessel_name", 1)], [("vessel_group", 1)], [("max_risk_score", -1)],
                     [("flagged", 1)], [("location", "2dsphere")]]),
        "vessel_positions": (lambda: read_csv(spark, "vessel_positions")
                             # denormalise name / risk so the map API needs no per-request join
                             .join(F.broadcast(read_csv(spark, "vessel_summary").select(
                                 "mmsi", F.col("vessel_name").alias("n"), F.col("max_risk_score").alias("r"),
                                 F.col("flagged").alias("f"))), "mmsi", "left")
                             .select(F.col("mmsi"), F.col("bucket").alias("t"), F.col("vessel_group").alias("g"),
                                     F.col("lat").alias("y"), F.col("lon").alias("x"), F.col("sog").alias("s"),
                                     F.col("cog").alias("c"), F.col("status").alias("st"),
                                     F.col("port_id").alias("p"), F.col("state").alias("z"),
                                     F.col("waiting").alias("w"), "n", "r", "f"),
                             [[("t", 1), ("g", 1)], [("mmsi", 1), ("t", 1)]]),
        "vessel_tracks": (lambda: read_csv(spark, "vessel_track_points")
                          .groupBy("mmsi").agg(
                              F.first("vessel_group").alias("vessel_group"),
                              F.array_sort(F.collect_list(F.array(F.col("epoch").cast("double"), F.col("lon"),
                                                                  F.col("lat"), F.coalesce(F.col("sog"), F.lit(-1.0)))))
                              .alias("points"))
                          .withColumn("n_points", F.size("points")),
                          [[("mmsi", 1)]]),
        "anomalies": (lambda: read_csv(spark, "gap_risk")
                      .withColumn("start_location", _geo("start_lon", "start_lat"))
                      .withColumn("end_location", _geo("end_lon", "end_lat")),
                      [[("risk_score", -1)], [("risk_level", 1)], [("mmsi", 1)], [("start_epoch", 1)],
                       [("vessel_group", 1)], [("start_nearest_port", 1)], [("start_location", "2dsphere")],
                       [("gap_h", -1)], [("start_dist_to_port_nm", 1)], [("event_date", 1)]]),
        "ais_visibility": (lambda: read_csv(spark, "ais_visibility")
                           .withColumn("neighbours", F.from_json("neighbours_json", NEIGHBOURS_SCHEMA))
                           .withColumn("target_timeline", F.from_json("target_timeline_json", "array<long>"))
                           .drop("neighbours_json", "target_timeline_json"),
                           [[("gap_id", 1)], [("mmsi", 1)], [("visibility", 1)], [("risk_score", -1)]]),
        "identity_events": (lambda: read_csv(spark, "identity_events"),
                            [[("mmsi", 1), ("epoch", 1)], [("event_type", 1)]]),
        "identity_vessels": (lambda: read_csv(spark, "identity_vessels"),
                             [[("mmsi", 1)], [("flagged", 1), ("conflict_events", -1)]]),
        "cluster_profiles": (lambda: read_csv(spark, "cluster_profiles"), [[("cluster", 1)]]),
        "daily_trends": (lambda: read_csv(spark, "daily_trends"), [[("date", 1)]]),
        "daily_group_activity": (lambda: read_csv(spark, "daily_group_activity"), [[("date", 1)]]),
        "hourly_activity": (lambda: read_csv(spark, "hourly_activity"), [[("hour_epoch", 1)]]),
    }


def load_small_documents(spark, uri: str, db: str):
    """Pipeline metrics and model parameters: a handful of documents."""
    from pymongo import MongoClient
    client = MongoClient(uri)
    d = client[db]

    metrics = (spark.read.option("header", "true").option("escape", '"')
               .csv(lake_path("pipeline_metrics")).collect())
    stages: dict[str, dict] = {}
    for r in metrics:
        stages.setdefault(r["stage"], {"stage": r["stage"]})[r["metric"]] = json.loads(r["value"])
    d.pipeline_metrics.drop()
    if stages:
        d.pipeline_metrics.insert_many(list(stages.values()))

    model = spark.read.text(lake_path("kmeans_model")).first()
    d.ml_model.drop()
    if model:
        doc = json.loads(model["value"])
        doc["cluster_sizes"] = {str(k): v for k, v in doc["cluster_sizes"].items()}
        doc["silhouette_by_k"] = {str(k): v for k, v in doc["silhouette_by_k"].items()}
        d.ml_model.insert_one(doc)
    print(f"[mongo] pipeline_metrics         {len(stages):>10,d} documents")
    client.close()


def run(spark=None, only: list[str] | None = None):
    cfg = load_config()["mongodb"]
    spark = spark or get_spark("11_load_mongodb")
    with Timer() as t:
        jobs = build_jobs(spark)
        for name, (builder, indexes) in jobs.items():
            if only and name not in only:
                continue
            write_collection(builder(), name, indexes, cfg["uri"], cfg["database"])
        if not only or "pipeline_metrics" in only:
            load_small_documents(spark, cfg["uri"], cfg["database"])
    print(f"[mongo] loaded into {cfg['uri']}/{cfg['database']} in {t.seconds}s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default=None, help="comma separated collection names")
    args = ap.parse_args()
    run(only=args.only.split(",") if args.only else None)


if __name__ == "__main__":
    main()
