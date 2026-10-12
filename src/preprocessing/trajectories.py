"""Stage 2 - Vessel trajectories (Spark window functions).

processed/clean_ais -> processed/vessel_tracks

For every MMSI the fixes are ordered by timestamp and each fix is enriched
with the previous fix, giving per-segment movement features:

  dt_s        seconds since previous fix
  dist_nm     great-circle distance from previous fix
  implied_kn  dist_nm / dt (null when dt = 0)
  cog_change  absolute change of course over ground
  move_bearing bearing actually travelled from previous fix

The shuffle is partitioned by MMSI, so all fixes of one vessel are processed
by the same task - this is the distributed equivalent of 'group by vessel,
sort by time'.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.geo import angle_diff_deg, bearing_deg, haversine_nm
from src.common.metrics import Timer, record_metrics
from src.common.schemas import CLEAN_AIS_SCHEMA, TRACK_SCHEMA
from src.common.spark import get_spark, read_csv, write_csv


def add_segment_features(df):
    """Add previous-fix features to a clean AIS DataFrame (pure function, testable)."""
    w = Window.partitionBy("mmsi").orderBy("epoch", "lat", "lon")
    df = (df
          .withColumn("prev_epoch", F.lag("epoch").over(w))
          .withColumn("prev_lat", F.lag("lat").over(w))
          .withColumn("prev_lon", F.lag("lon").over(w))
          .withColumn("prev_sog", F.lag("sog").over(w))
          .withColumn("prev_cog", F.lag("cog").over(w)))
    df = (df
          .withColumn("dt_s", F.col("epoch") - F.col("prev_epoch"))
          .withColumn("dist_nm", F.round(haversine_nm("prev_lat", "prev_lon", "lat", "lon"), 4))
          .withColumn("implied_kn", F.when(F.col("dt_s") > 0,
                                           F.round(F.col("dist_nm") / (F.col("dt_s") / 3600.0), 3)))
          .withColumn("cog_change", F.round(angle_diff_deg(F.col("cog"), F.col("prev_cog")), 1))
          .withColumn("move_bearing", F.when(F.col("dist_nm") > 0.01,
                                             F.round(bearing_deg("prev_lat", "prev_lon", "lat", "lon"), 1))))
    return df


def run(spark=None):
    spark = spark or get_spark("02_trajectories")
    with Timer() as t:
        clean = read_csv(spark, "clean_ais", CLEAN_AIS_SCHEMA)
        tracks = add_segment_features(clean)
        cols = [f.name for f in CLEAN_AIS_SCHEMA.fields if f.name != "event_date"] + [
            "prev_epoch", "prev_lat", "prev_lon", "prev_sog", "prev_cog",
            "dt_s", "dist_nm", "implied_kn", "cog_change", "move_bearing", "event_date"]
        write_csv(tracks.select(*cols), "vessel_tracks", partition_by=["event_date"])

        written = read_csv(spark, "vessel_tracks", TRACK_SCHEMA)
        # Reporting-interval distribution by transceiver class - used to
        # justify the AIS gap thresholds in config/pipeline.yaml.
        q = (written.filter(F.col("dt_s").isNotNull())
             .groupBy("transceiver")
             .agg(F.count(F.lit(1)).alias("segments"),
                  F.expr("percentile_approx(dt_s, array(0.5, 0.9, 0.95, 0.99, 0.999), 2000)").alias("q"),
                  F.sum(F.when(F.col("dt_s") >= 3600, 1).otherwise(0)).alias("over_1h"),
                  F.sum(F.when(F.col("dt_s") >= 7200, 1).otherwise(0)).alias("over_2h"))
             .collect())
        total = written.count()
    record_metrics(spark, "trajectories", {
        "track_rows": total,
        "reporting_interval_seconds": {
            r["transceiver"]: {"segments": r["segments"],
                               "p50": r["q"][0], "p90": r["q"][1], "p95": r["q"][2],
                               "p99": r["q"][3], "p999": r["q"][4],
                               "segments_over_1h": r["over_1h"], "segments_over_2h": r["over_2h"]}
            for r in q},
        "elapsed_seconds": t.seconds,
    })


if __name__ == "__main__":
    run()
