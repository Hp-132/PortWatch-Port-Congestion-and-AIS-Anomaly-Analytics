"""Stage 1 - AIS preprocessing (Spark).

raw AIS CSV  ->  processed/clean_ais        (partitioned by event_date)
             ->  processed/vessel_registry  (one row per MMSI)

Cleaning rules (all counted and recorded as metrics):
  * timestamp parsed from 'yyyy-MM-dd HH:mm:ss' (UTC); unparseable rows dropped
  * MMSI must be 9 digits with a valid MID (200000000-799999999)
  * latitude in [-90, 90], longitude in [-180, 180], not at 'null island' (0,0)
  * SOG outside [0, max_valid_sog] -> null (position kept)
  * COG / heading outside [0, 360) -> null
  * exact duplicates of (mmsi, ts, lat, lon) removed; same-time/different-
    position fixes are KEPT because they are evidence for identity checks
  * static attributes (type, dimensions, name) are harmonised per MMSI using
    the most frequent reported value, so a vessel has one consistent group
"""
from __future__ import annotations

from pyspark.sql import functions as F

from src.common.config import load_config
from src.common.metrics import Timer, record_metrics
from src.common.paths import raw_input
from src.common.schemas import CLEAN_AIS_SCHEMA, RAW_AIS_SCHEMA, vessel_group_expr
from src.common.spark import get_spark, read_csv, write_csv


def run(spark=None):
    cfg = load_config()["cleaning"]
    spark = spark or get_spark("01_clean_ais")

    with Timer() as t:
        raw = spark.read.option("header", "true").schema(RAW_AIS_SCHEMA).csv(raw_input())

        df = (raw
              .withColumn("ts", F.to_timestamp("base_date_time", "yyyy-MM-dd HH:mm:ss"))
              .withColumn("f_bad_ts", F.col("ts").isNull())
              .withColumn("f_bad_mmsi", ~F.col("mmsi").between(cfg["mmsi_min"], cfg["mmsi_max"])
                          | F.col("mmsi").isNull())
              .withColumn("f_bad_coord",
                          F.col("latitude").isNull() | F.col("longitude").isNull()
                          | ~F.col("latitude").between(-90, 90) | ~F.col("longitude").between(-180, 180)
                          | ((F.abs("latitude") < cfg["null_island_tolerance_deg"])
                             & (F.abs("longitude") < cfg["null_island_tolerance_deg"])))
              .withColumn("f_bad_sog", F.col("sog").isNotNull()
                          & ~F.col("sog").between(0, cfg["max_valid_sog_kn"]))
              .withColumn("f_bad_cog", F.col("cog").isNotNull() & ~((F.col("cog") >= 0) & (F.col("cog") < 360)))
              .withColumn("f_bad_hdg", F.col("heading").isNotNull()
                          & ~((F.col("heading") >= 0) & (F.col("heading") < 360))))

        # One pass to count every rule violation on the raw data.
        flag_cols = ["f_bad_ts", "f_bad_mmsi", "f_bad_coord", "f_bad_sog", "f_bad_cog", "f_bad_hdg"]
        counts = df.agg(F.count(F.lit(1)).alias("raw_rows"),
                        *[F.sum(F.col(c).cast("int")).alias(c) for c in flag_cols]).first().asDict()

        valid = (df.filter(~F.col("f_bad_ts") & ~F.col("f_bad_mmsi") & ~F.col("f_bad_coord"))
                 .withColumn("sog", F.when(F.col("f_bad_sog"), None).otherwise(F.col("sog")))
                 .withColumn("cog", F.when(F.col("f_bad_cog"), None).otherwise(F.col("cog")))
                 .withColumn("heading", F.when(F.col("f_bad_hdg"), None).otherwise(F.col("heading"))))

        # ---- vessel registry: harmonised static data per MMSI -------------
        def blank_to_null(c):
            return F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))

        registry = (valid
                    .groupBy("mmsi")
                    .agg(F.mode(blank_to_null("vessel_name")).alias("vessel_name"),
                         F.mode(blank_to_null("imo")).alias("imo"),
                         F.mode(blank_to_null("call_sign")).alias("call_sign"),
                         F.mode("vessel_type").alias("vessel_type"),
                         F.countDistinct("vessel_type").alias("n_vessel_types"),
                         F.countDistinct(blank_to_null("vessel_name")).alias("n_names"),
                         F.expr("percentile_approx(length, 0.5)").alias("length"),
                         F.expr("percentile_approx(width, 0.5)").alias("width"),
                         F.max("draft").alias("max_draft"),
                         F.mode("transceiver").alias("transceiver"),
                         F.count(F.lit(1)).alias("raw_points"),
                         F.min("ts").alias("first_seen"),
                         F.max("ts").alias("last_seen"))
                    .withColumn("vessel_group", vessel_group_expr(F.col("vessel_type"))))
        registry = registry.cache()
        write_csv(registry, "vessel_registry", coalesce=4)

        clean = (valid
                 .dropDuplicates(["mmsi", "ts", "latitude", "longitude"])
                 .join(F.broadcast(registry.select("mmsi", "vessel_group",
                                                   F.col("transceiver").alias("reg_transceiver"))),
                       "mmsi", "left")
                 .select("mmsi", "ts",
                         F.unix_timestamp("ts").alias("epoch"),
                         F.round("latitude", 5).alias("lat"),
                         F.round("longitude", 5).alias("lon"),
                         "sog", "cog", "heading", "status", "vessel_group",
                         F.coalesce("reg_transceiver", "transceiver").alias("transceiver"),
                         F.date_format("ts", "yyyy-MM-dd").alias("event_date")))
        write_csv(clean, "clean_ais", partition_by=["event_date"])

        # Verify what was actually written (reads the CSV back).
        written = read_csv(spark, "clean_ais", CLEAN_AIS_SCHEMA)
        w = written.agg(F.count(F.lit(1)).alias("rows"), F.countDistinct("mmsi").alias("vessels"),
                        F.min("epoch").alias("min_epoch"), F.max("epoch").alias("max_epoch")).first()
        reg_count = registry.count()
        groups = {r["vessel_group"]: r["count"] for r in registry.groupBy("vessel_group").count().collect()}

    record_metrics(spark, "clean_ais", {
        "raw_rows": counts["raw_rows"],
        "invalid_timestamp_rows": counts["f_bad_ts"],
        "invalid_mmsi_rows": counts["f_bad_mmsi"],
        "invalid_coordinate_rows": counts["f_bad_coord"],
        "sog_nullified": counts["f_bad_sog"],
        "cog_nullified": counts["f_bad_cog"],
        "heading_nullified": counts["f_bad_hdg"],
        "clean_rows": w["rows"],
        "removed_rows": counts["raw_rows"] - w["rows"],
        "vessels": w["vessels"],
        "registry_vessels": reg_count,
        "vessels_by_group": groups,
        "window_start_epoch": w["min_epoch"],
        "window_end_epoch": w["max_epoch"],
        "elapsed_seconds": t.seconds,
    })
    registry.unpersist()


if __name__ == "__main__":
    run()
