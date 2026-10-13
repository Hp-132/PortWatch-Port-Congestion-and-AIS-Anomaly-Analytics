"""Stage 3 - Port zone assignment and port-visit segmentation (Spark).

processed/clean_ais -> processed/port_points  (commercial fixes inside port zones)
                    -> processed/port_visits  (one row per vessel visit)

Visit model (arrival -> anchorage -> berth/operations -> departure):

  arrival_ts        first fix inside the port's PORT or ANCHORAGE zone
  anchor phase      time in ANCHOR state before the first BERTH fix  (= waiting)
  berth_start/end   first / last BERTH fix                           (= dwell)
  departure_ts      last fix inside the zones
  turnaround        departure_ts - arrival_ts

A new visit starts when the vessel appears at a different port or has been
absent from the port's zones for more than ``visit_break_hours``.

Time spent in a state is measured as the interval to the next fix of the same
visit, capped at 3 h so a reporting gap is not counted as anchor time.

A run of consecutive BERTH fixes only counts as a berth episode if it lasts at
least ``min_berth_minutes``; shorter runs (a vessel slowing in the channel)
are relabelled MOVING so they cannot start the berth phase early.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.analytics.ports import assign_zones, ports_df
from src.common.config import load_config
from src.common.metrics import Timer, read_metrics, record_metrics
from src.common.schemas import CLEAN_AIS_SCHEMA
from src.common.spark import get_spark, read_csv, write_csv

MAX_STATE_INTERVAL_S = 3 * 3600


def build_visits(points, visit_break_hours: float, min_visit_minutes: float,
                 window_start: int, window_end: int, censor_margin_hours: float,
                 min_berth_minutes: float = 30):
    """Segment zone-assigned points into visits and compute phase durations.

    ``points`` columns: mmsi, epoch, port_id, state (+ anything else).
    Pure DataFrame logic so it can be unit-tested on tiny inputs.
    """
    w = Window.partitionBy("mmsi").orderBy("epoch")
    pts = (points
           .withColumn("_prev_port", F.lag("port_id").over(w))
           .withColumn("_prev_epoch", F.lag("epoch").over(w))
           .withColumn("_new", F.when(F.col("_prev_port").isNull()
                                      | (F.col("_prev_port") != F.col("port_id"))
                                      | ((F.col("epoch") - F.col("_prev_epoch")) > visit_break_hours * 3600), 1)
                       .otherwise(0))
           .withColumn("visit_seq", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0))))

    wv = Window.partitionBy("mmsi", "visit_seq").orderBy("epoch")
    pts = (pts
           .withColumn("_next_epoch", F.lead("epoch").over(wv))
           .withColumn("state_s", F.least(F.coalesce(F.col("_next_epoch") - F.col("epoch"), F.lit(0)),
                                          F.lit(MAX_STATE_INTERVAL_S)))
           # Drop berth runs shorter than min_berth_minutes (relabel MOVING)
           .withColumn("_chg", F.when(F.lag("state").over(wv) == F.col("state"), 0).otherwise(1))
           .withColumn("_run", F.sum("_chg").over(wv.rowsBetween(Window.unboundedPreceding, 0)))
           .withColumn("_run_s", F.sum("state_s").over(Window.partitionBy("mmsi", "visit_seq", "_run")))
           .withColumn("state", F.when((F.col("state") == "BERTH") & (F.col("_run_s") < min_berth_minutes * 60),
                                       F.lit("MOVING")).otherwise(F.col("state")))
           .withColumn("_berth_start", F.min(F.when(F.col("state") == "BERTH", F.col("epoch")))
                       .over(Window.partitionBy("mmsi", "visit_seq"))))

    is_berth = F.col("state") == "BERTH"
    is_anchor = F.col("state") == "ANCHOR"
    visits = (pts.groupBy("mmsi", "visit_seq")
              .agg(F.first("port_id").alias("port_id"),
                   F.min("epoch").alias("arrival_epoch"),
                   F.max("epoch").alias("departure_epoch"),
                   F.min(F.when(is_anchor, F.col("epoch"))).alias("anchor_start_epoch"),
                   F.max(F.when(is_anchor & (F.col("_berth_start").isNull()
                                             | (F.col("epoch") < F.col("_berth_start"))),
                                F.col("epoch"))).alias("anchor_end_epoch"),
                   F.min(F.when(is_berth, F.col("epoch"))).alias("berth_start_epoch"),
                   F.max(F.when(is_berth, F.col("epoch"))).alias("berth_end_epoch"),
                   F.sum(F.when(is_anchor & (F.col("_berth_start").isNull()
                                             | (F.col("epoch") < F.col("_berth_start"))),
                                F.col("state_s")).otherwise(0)).alias("anchor_before_berth_s"),
                   F.sum(F.when(is_anchor, F.col("state_s")).otherwise(0)).alias("anchor_total_s"),
                   F.sum(F.when(is_berth, F.col("state_s")).otherwise(0)).alias("berth_observed_s"),
                   F.count(F.lit(1)).alias("fixes"),
                   F.sum(is_berth.cast("int")).alias("berth_fixes"),
                   F.sum(is_anchor.cast("int")).alias("anchor_fixes")))

    margin = censor_margin_hours * 3600
    has_berth = F.col("berth_start_epoch").isNotNull()
    has_anchor = F.col("anchor_start_epoch").isNotNull()
    duration_min = (F.col("departure_epoch") - F.col("arrival_epoch")) / 60.0
    visits = (visits
              .withColumn("visit_type",
                          F.when(has_berth, F.lit("BERTHED"))
                          .when(has_anchor, F.lit("ANCHORED_ONLY"))
                          .otherwise(F.lit("PASS_THROUGH")))
              .withColumn("visit_type", F.when((F.col("visit_type") != "PASS_THROUGH")
                                               & (duration_min < min_visit_minutes), F.lit("PASS_THROUGH"))
                          .otherwise(F.col("visit_type")))
              .withColumn("censored_arrival", F.col("arrival_epoch") <= window_start + margin)
              .withColumn("censored_departure", F.col("departure_epoch") >= window_end - margin)
              .withColumn("waiting_h", F.when(has_berth, F.round(F.col("anchor_before_berth_s") / 3600, 3)))
              .withColumn("dwell_h", F.when(has_berth, F.round(
                  (F.col("berth_end_epoch") - F.col("berth_start_epoch")) / 3600, 3)))
              .withColumn("turnaround_h", F.round((F.col("departure_epoch") - F.col("arrival_epoch")) / 3600, 3))
              .withColumn("anchor_only_h", F.when(~has_berth & has_anchor,
                                                  F.round(F.col("anchor_total_s") / 3600, 3)))
              .withColumn("complete", ~F.col("censored_arrival") & ~F.col("censored_departure")))
    return pts.drop("_prev_port", "_prev_epoch", "_new", "_next_epoch", "_chg", "_run", "_run_s",
                    "_berth_start"), visits


def run(spark=None):
    cfg = load_config()
    pc = cfg["ports"]
    spark = spark or get_spark("03_port_visits")
    win = read_metrics(spark, "clean_ais")

    with Timer() as t:
        clean = read_csv(spark, "clean_ais", CLEAN_AIS_SCHEMA)
        commercial = (clean.filter(F.col("vessel_group").isin(pc["commercial_groups"]))
                      .select("mmsi", "ts", "epoch", "lat", "lon", "sog", "status", "vessel_group"))
        ports = ports_df(spark)
        points = assign_zones(commercial, ports, cfg["trajectory"]["stationary_sog_kn"])

        pts, visits = build_visits(points, pc["visit_break_hours"], pc["min_visit_minutes"],
                                   win["window_start_epoch"], win["window_end_epoch"],
                                   pc["censor_margin_hours"], pc["min_berth_minutes"])
        pts = pts.cache()
        write_csv(pts.withColumn("event_date", F.date_format("ts", "yyyy-MM-dd")),
                  "port_points", partition_by=["event_date"])

        def ts_col(c):
            return F.from_unixtime(F.col(c)).cast("timestamp")

        visits_out = (visits
                      .withColumn("visit_id", F.concat_ws("-", F.col("mmsi").cast("string"),
                                                          F.col("port_id"), F.col("arrival_epoch").cast("string")))
                      .withColumn("arrival_ts", ts_col("arrival_epoch"))
                      .withColumn("departure_ts", ts_col("departure_epoch"))
                      .withColumn("anchor_start_ts", ts_col("anchor_start_epoch"))
                      .withColumn("anchor_end_ts", ts_col("anchor_end_epoch"))
                      .withColumn("berth_start_ts", ts_col("berth_start_epoch"))
                      .withColumn("berth_end_ts", ts_col("berth_end_epoch")))
        write_csv(visits_out, "port_visits", coalesce=4)

        stats = (visits_out.groupBy("visit_type").agg(F.count(F.lit(1)).alias("n"),
                                                     F.sum(F.col("complete").cast("int")).alias("complete"))
                 .collect())
        n_points = pts.count()
        states = {r["state"]: r["count"] for r in pts.groupBy("state").count().collect()}
        pts.unpersist()

    record_metrics(spark, "port_visits", {
        "port_zone_points": n_points,
        "points_by_state": states,
        "visits_by_type": {r["visit_type"]: {"visits": r["n"], "complete": r["complete"]} for r in stats},
        "elapsed_seconds": t.seconds,
    })


if __name__ == "__main__":
    run()
