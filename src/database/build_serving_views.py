"""Stage 10 - Build map-ready / dashboard-ready serving views (Spark).

The dashboard must not re-process 48M fixes on every request, so Spark
pre-aggregates everything the UI needs:

  output/vessel_summary         one row per MMSI (registry + movement + visits + risk)
  output/vessel_positions       last fix per vessel per hour  (map time slider)
  output/vessel_track_points    last fix per vessel per 15 min (track drawing)
  output/daily_trends           one row per day (traffic, gaps, arrivals, PCI)
  output/daily_group_activity   active vessels / messages per day and vessel group
  output/hourly_activity        messages / active vessels per hour
"""
from __future__ import annotations

from pyspark.sql import functions as F

from src.analytics.ports import assign_zones, ports_df
from src.common.config import load_config
from src.common.metrics import Timer, record_metrics
from src.common.schemas import CLEAN_AIS_SCHEMA, TRACK_SCHEMA
from src.common.spark import get_spark, read_csv, write_csv


def last_fix_per_bucket(df, minutes: int):
    b = minutes * 60
    return (df.withColumn("bucket", (F.floor(F.col("epoch") / b) * b).cast("long"))
            .groupBy("mmsi", "bucket")
            .agg(F.max_by(F.struct("epoch", "lat", "lon", "sog", "cog", "heading", "status"), "epoch").alias("f"),
                 F.first("vessel_group").alias("vessel_group"))
            .select("mmsi", "bucket", "vessel_group", "f.*"))


def run(spark=None):
    cfg = load_config()["serving"]
    spark = spark or get_spark("10_serving_views")
    with Timer() as t:
        tracks = read_csv(spark, "vessel_tracks", TRACK_SCHEMA)
        clean = read_csv(spark, "clean_ais", CLEAN_AIS_SCHEMA)
        registry = read_csv(spark, "vessel_registry")
        visits = read_csv(spark, "turnaround_visits")
        gaps = read_csv(spark, "gap_risk")
        ident = read_csv(spark, "identity_vessels")
        daily_cong = read_csv(spark, "congestion_daily")

        # ---- vessel summary ---------------------------------------------
        plausible = (F.col("dt_s") > 0) & (F.col("implied_kn") <= 40)
        movement = tracks.groupBy("mmsi").agg(
            F.count(F.lit(1)).alias("fixes"),
            F.round(F.sum(F.when(plausible, F.col("dist_nm")).otherwise(0)), 1).alias("distance_nm"),
            F.round(F.avg(F.when(F.col("sog") >= 2, F.col("sog"))), 2).alias("avg_underway_sog"),
            F.max("sog").alias("max_sog"),
            F.round(F.avg((F.col("sog") >= 2).cast("double")) * 100, 1).alias("pct_time_underway"),
            F.countDistinct("event_date").alias("active_days"),
            # AIS reporting frequency: median interval between consecutive
            # reports, and reports per hour over the vessel's active span
            F.percentile_approx("dt_s", 0.5).alias("median_report_interval_s"),
            F.percentile_approx("dt_s", 0.95).alias("p95_report_interval_s"),
            F.min("epoch").alias("first_epoch"), F.max("epoch").alias("last_epoch"),
            F.max_by(F.struct("ts", "lat", "lon", "sog", "cog", "heading", "status"), "epoch").alias("last"))
        v_visits = visits.groupBy("mmsi").agg(
            F.count(F.lit(1)).alias("port_visits"),
            F.array_sort(F.collect_set("port_id")).alias("ports_visited"),
            F.round(F.avg("turnaround_h"), 2).alias("avg_turnaround_h"),
            F.round(F.avg("waiting_h"), 2).alias("avg_waiting_h"),
            F.round(F.avg("dwell_h"), 2).alias("avg_dwell_h"))
        v_gaps = gaps.groupBy("mmsi").agg(
            F.count(F.lit(1)).alias("ais_gaps"),
            F.round(F.sum("gap_h"), 1).alias("total_gap_h"),
            F.max("risk_score").alias("max_risk_score"),
            F.sum((F.col("risk_level") == "High Priority").cast("int")).alias("high_priority_gaps"),
            F.sum((F.col("risk_level") == "Suspicious").cast("int")).alias("suspicious_gaps"))
        risk_level_expr = (F.when(F.col("max_risk_score") > 80, "High Priority")
                           .when(F.col("max_risk_score") > 60, "Suspicious")
                           .when(F.col("max_risk_score") > 30, "Watch").otherwise("Normal"))
        summary = (registry.join(movement, "mmsi", "left").join(v_visits, "mmsi", "left")
                   .join(v_gaps, "mmsi", "left")
                   .join(ident.select("mmsi", "conflict_events", "isolated_outliers", "flagged", "flag_label"),
                         "mmsi", "left")
                   .fillna(0, ["port_visits", "ais_gaps", "high_priority_gaps", "suspicious_gaps",
                               "conflict_events", "isolated_outliers", "max_risk_score"])
                   .fillna(False, ["flagged"])
                   .withColumn("vessel_risk_level", risk_level_expr)
                   .withColumn("last_ts", F.col("last.ts")).withColumn("last_lat", F.col("last.lat"))
                   .withColumn("last_lon", F.col("last.lon")).withColumn("last_sog", F.col("last.sog"))
                   .withColumn("last_cog", F.col("last.cog")).withColumn("last_heading", F.col("last.heading"))
                   .withColumn("last_status", F.col("last.status"))
                   .withColumn("ports_visited", F.concat_ws("|", "ports_visited"))
                   .withColumn("reports_per_hour", F.round(F.col("fixes") / F.greatest(
                       (F.col("last_epoch") - F.col("first_epoch")) / 3600.0, F.lit(1.0)), 2))
                   .drop("last"))
        write_csv(summary, "vessel_summary", coalesce=2)

        # ---- positions and tracks for the map ----------------------------
        # Hourly positions tagged with port-zone state so the map can show
        # waiting (anchored in a port's anchorage) and berthed commercial vessels.
        full = load_config()
        positions = last_fix_per_bucket(clean, cfg["position_bucket_minutes"])
        zones = (assign_zones(positions.select("mmsi", "bucket", "epoch", "lat", "lon", "sog", "status"),
                              ports_df(spark), full["trajectory"]["stationary_sog_kn"])
                 .select("mmsi", "bucket", "port_id", "zone", "state"))
        commercial = F.col("vessel_group").isin(full["ports"]["commercial_groups"])
        positions = (positions.join(zones, ["mmsi", "bucket"], "left")
                     .withColumn("waiting", (commercial & (F.col("state") == "ANCHOR")).cast("int")))
        write_csv(positions, "vessel_positions")
        write_csv(last_fix_per_bucket(clean, cfg["track_bucket_minutes"]), "vessel_track_points")

        # ---- trends -------------------------------------------------------
        traffic = clean.groupBy(F.col("event_date").alias("date")).agg(
            F.count(F.lit(1)).alias("messages"), F.countDistinct("mmsi").alias("active_vessels"),
            F.sum((F.col("transceiver") == "A").cast("int")).alias("class_a_messages"),
            F.sum((F.col("transceiver") == "B").cast("int")).alias("class_b_messages"),
            F.countDistinct(F.when(F.col("sog") >= 2, F.col("mmsi"))).alias("underway_vessels"))
        g = gaps.groupBy(F.date_format("start_ts", "yyyy-MM-dd").alias("date")).agg(
            F.count(F.lit(1)).alias("ais_gaps"),
            *[F.sum((F.col("risk_level") == lv).cast("int")).alias(f"gaps_{lv.lower().replace(' ', '_')}")
              for lv in ["Normal", "Watch", "Suspicious", "High Priority"]])
        c = daily_cong.groupBy("date").agg(
            F.sum("arrivals").alias("port_arrivals"), F.sum("departures").alias("port_departures"),
            F.round(F.avg("pci"), 1).alias("mean_daily_pci"), F.round(F.sum("avg_queue"), 1).alias("total_avg_queue"))
        trends = traffic.join(g, "date", "left").join(c, "date", "left").orderBy("date")
        write_csv(trends, "daily_trends", coalesce=1)

        grp = clean.groupBy(F.col("event_date").alias("date"), "vessel_group").agg(
            F.count(F.lit(1)).alias("messages"), F.countDistinct("mmsi").alias("vessels"))
        write_csv(grp.orderBy("date", "vessel_group"), "daily_group_activity", coalesce=1)

        hourly = clean.groupBy((F.floor(F.col("epoch") / 3600) * 3600).cast("long").alias("hour_epoch")).agg(
            F.count(F.lit(1)).alias("messages"), F.countDistinct("mmsi").alias("active_vessels"))
        write_csv(hourly.orderBy("hour_epoch"), "hourly_activity", coalesce=1)

        n_vessels = summary.count()
    record_metrics(spark, "serving_views", {"vessel_summaries": n_vessels, "elapsed_seconds": t.seconds})


if __name__ == "__main__":
    run()
