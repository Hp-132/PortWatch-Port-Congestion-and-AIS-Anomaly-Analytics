"""Stage 9 - Identity-consistency check (Spark).

Output label: "Potential MMSI identity inconsistency".
This NEVER proves identity swapping or spoofing. Position jumps are far more
often caused by GNSS errors, corrupted messages, receiver time-stamping or
two transponders mis-configured with the same MMSI.

processed/vessel_tracks -> analytics/dark_vessel/identity_events
                        -> analytics/dark_vessel/identity_vessels

For consecutive fixes of one MMSI (dt <= max_dt_minutes, distance >= min_jump_nm):
  implied speed = distance / dt  (dt = 0 with different positions = simultaneous
                                  reports from two places)
  event if implied speed > ceiling for the vessel group AND
           implied speed > sog_mismatch_factor * max(reported SOG) + 5 kn
Context recorded per event: reported SOG, COG change, movement bearing vs COG,
vessel group, whether the jump starts/ends inside a port zone.

Event classification using the following fix (i+1):
  isolated_outlier   fix i is far from both neighbours but i-1 -> i+1 is
                     consistent: a single bad position (likely GNSS/decoding)
  track_conflict     otherwise - positions alternate between distinct tracks
                     or the jump is sustained

A vessel is flagged when it has >= min_events_to_flag track_conflict events.
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.analytics.ports import ports_df
from src.common.config import load_config
from src.common.geo import angle_diff_deg, haversine_nm
from src.common.metrics import Timer, record_metrics
from src.common.schemas import TRACK_SCHEMA
from src.common.spark import get_spark, read_csv, write_csv

FLAG_LABEL = "Potential MMSI identity inconsistency"


def speed_ceiling_expr(ceilings: dict) -> F.Column:
    expr = F.lit(float(ceilings["default"]))
    for group, v in ceilings.items():
        if group != "default":
            expr = F.when(F.col("vessel_group") == group, F.lit(float(v))).otherwise(expr)
    return expr


def detect_events(tracks, cfg_id: dict):
    ceiling = speed_ceiling_expr(cfg_id["implausible_speed_kn"])
    max_sog = F.greatest(F.coalesce("prev_sog", F.lit(0.0)), F.coalesce("sog", F.lit(0.0)))
    impossible = F.when(F.col("dt_s") == 0, F.lit(True)).otherwise(F.col("implied_kn") > ceiling)
    cand = (tracks
            .filter(F.col("dt_s").isNotNull() & (F.col("dt_s") <= cfg_id["max_dt_minutes"] * 60)
                    & (F.col("dist_nm") >= cfg_id["min_jump_nm"]))
            .filter(impossible & ((F.col("dt_s") == 0)
                                  | (F.col("implied_kn") > cfg_id["sog_mismatch_factor"] * max_sog + 5))))
    return cand


def classify_events(tracks, cand, min_jump_nm: float):
    """Separate isolated outliers from track conflicts.

    A fix is a *spike* when it is far from both neighbours while the
    neighbours are close to each other. A spike whose neighbours are not
    spikes is an isolated outlier (one bad position): both the jump out and
    the jump back are labelled ``isolated_outlier``. Consecutive spikes mean
    the positions alternate between two tracks -> ``track_conflict``.
    """
    affected = cand.select("mmsi").distinct()
    w = Window.partitionBy("mmsi").orderBy("epoch", "lat", "lon")
    t = (tracks.join(F.broadcast(affected), "mmsi")
         .select("mmsi", "epoch", "lat", "lon", "prev_lat", "prev_lon", "dist_nm")
         .withColumn("next_lat", F.lead("lat").over(w))
         .withColumn("next_lon", F.lead("lon").over(w))
         .withColumn("next_dist", F.lead("dist_nm").over(w)))
    d_prev_next = haversine_nm(F.col("prev_lat"), F.col("prev_lon"), F.col("next_lat"), F.col("next_lon"))
    t = (t.withColumn("dist_prev_to_next_nm", F.round(d_prev_next, 3))
         .withColumn("spike", F.coalesce((F.col("dist_nm") >= min_jump_nm) & (F.col("next_dist") >= min_jump_nm)
                                         & (d_prev_next < min_jump_nm), F.lit(False))))
    t = t.withColumn("iso_spike", F.col("spike") & ~F.coalesce(F.lag("spike").over(w), F.lit(False))
                     & ~F.coalesce(F.lead("spike").over(w), F.lit(False)))
    t = (t.withColumn("prev_iso_spike", F.coalesce(F.lag("iso_spike").over(w), F.lit(False)))
         .select("mmsi", "epoch", "lat", "lon", "iso_spike", "prev_iso_spike", "dist_prev_to_next_nm"))
    ev = cand.join(t, ["mmsi", "epoch", "lat", "lon"], "left")
    return (ev.withColumn("event_type",
                          F.when(F.col("iso_spike") | F.col("prev_iso_spike"), F.lit("isolated_outlier"))
                          .otherwise(F.lit("track_conflict")))
            .drop("iso_spike", "prev_iso_spike"))


def run(spark=None):
    cfg = load_config()
    ci = cfg["identity"]
    spark = spark or get_spark("09_identity_consistency")
    with Timer() as t:
        tracks = read_csv(spark, "vessel_tracks", TRACK_SCHEMA)
        cand = detect_events(tracks, ci).cache()
        events = classify_events(tracks, cand, ci["min_jump_nm"])

        ports = ports_df(spark).select("port_id", "port_lat", "port_lon", "port_radius_nm")
        in_port = (events.select("mmsi", "epoch", "lat", "lon").crossJoin(F.broadcast(ports))
                   .filter(haversine_nm(F.col("lat"), F.col("lon"), F.col("port_lat"), F.col("port_lon"))
                           <= F.col("port_radius_nm"))
                   .groupBy("mmsi", "epoch", "lat", "lon").agg(F.first("port_id").alias("port_id")))
        events = (events.join(in_port, ["mmsi", "epoch", "lat", "lon"], "left")
                  .withColumn("in_port_zone", F.col("port_id").isNotNull())
                  .withColumn("bearing_vs_cog_deg", F.round(angle_diff_deg(F.col("move_bearing"), F.col("prev_cog")), 1))
                  .withColumn("event_id", F.concat_ws("-", F.col("mmsi").cast("string"), F.col("epoch").cast("string"),
                                                      F.round("lat", 3).cast("string")))
                  .select("event_id", "mmsi", "vessel_group", "transceiver", "ts", "epoch",
                          "prev_lat", "prev_lon", "lat", "lon", "dt_s", "dist_nm", "implied_kn",
                          "prev_sog", "sog", "prev_cog", "cog", "cog_change", "move_bearing",
                          "bearing_vs_cog_deg", "event_type", "dist_prev_to_next_nm", "in_port_zone", "port_id")
                  .cache())
        write_csv(events, "identity_events", coalesce=2)

        vessels = (events.groupBy("mmsi", "vessel_group")
                   .agg(F.count(F.lit(1)).alias("events"),
                        F.sum((F.col("event_type") == "track_conflict").cast("int")).alias("conflict_events"),
                        F.sum((F.col("event_type") == "isolated_outlier").cast("int")).alias("isolated_outliers"),
                        F.sum((F.col("dt_s") == 0).cast("int")).alias("simultaneous_reports"),
                        F.round(F.max("implied_kn"), 1).alias("max_implied_kn"),
                        F.round(F.max("dist_nm"), 2).alias("max_jump_nm"),
                        F.countDistinct(F.to_date("ts")).alias("days_with_events"),
                        F.sum(F.col("in_port_zone").cast("int")).alias("events_in_port"),
                        F.min("ts").alias("first_event_ts"), F.max("ts").alias("last_event_ts"))
                   .withColumn("flagged", F.col("conflict_events") >= ci["min_events_to_flag"])
                   .withColumn("flag_label", F.when(F.col("flagged"), F.lit(FLAG_LABEL)))
                   .withColumn("consistency_score", F.round(F.least(F.lit(100.0),
                       10 * F.col("conflict_events") + 5 * F.col("simultaneous_reports")
                       + 2 * F.col("isolated_outliers")), 0).cast("int")))
        write_csv(vessels, "identity_vessels", coalesce=1)

        s = vessels.agg(F.count(F.lit(1)).alias("vessels_with_events"),
                        F.sum(F.col("flagged").cast("int")).alias("flagged"),
                        F.sum("events").alias("events"), F.sum("conflict_events").alias("conflicts"),
                        F.sum("isolated_outliers").alias("isolated")).first()
        cand.unpersist(); events.unpersist()

    record_metrics(spark, "identity", {
        "events": s["events"], "track_conflict_events": s["conflicts"], "isolated_outlier_events": s["isolated"],
        "vessels_with_events": s["vessels_with_events"], "vessels_flagged": s["flagged"],
        "flag_label": FLAG_LABEL, "elapsed_seconds": t.seconds,
    })


if __name__ == "__main__":
    run()
