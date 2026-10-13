"""Stage 6 - AIS gap extraction and contextual features (Spark).

AIS Dark-Vessel Anomaly Detection, step 1.
An AIS gap is NOT evidence of wrongdoing: most gaps are caused by receiver
coverage, transponder power settings or reduced Class B reporting. The
features below give each gap *context* so that unusual gaps can be ranked
for analyst review.

processed/vessel_tracks -> processed/ais_gaps, ml/features/gap_features

Per gap (fix i-1 -> fix i of the same MMSI with dt >= class threshold):
  gap_h                 duration
  sog_before/after      reported speed at both ends
  dist_nm, implied_kn   distance covered and the speed that implies
  speed_mismatch_kn     |implied speed - mean reported speed|
  course_change_deg     COG change across the gap
  start/end dist_to_port_nm, nearest port
  density_start         distinct vessels received in the same 0.25 deg cell
                        and hour - a proxy for receiver coverage
  local_hour, night     local solar time at gap start
  vessel_gaps_per_day   historical gap frequency of the vessel in the week
"""
from __future__ import annotations

from pyspark.sql import functions as F

from src.analytics.ports import nearest_port_distance, ports_df
from src.common.config import load_config
from src.common.geo import local_solar_hour
from src.common.metrics import Timer, record_metrics
from src.common.schemas import CLEAN_AIS_SCHEMA, TRACK_SCHEMA
from src.common.spark import get_spark, read_csv, write_csv

FEATURE_COLS = ["f_log_gap_h", "f_log_dist", "f_implied_kn", "f_sog_before", "f_sog_after",
                "f_speed_mismatch", "f_course_change", "f_log_port_dist", "f_log_density",
                "f_night", "f_log_gap_rate"]


def extract_gaps(tracks, class_a_min: float, class_b_min: float):
    threshold_s = F.when(F.col("transceiver") == "B", class_b_min * 60).otherwise(class_a_min * 60)
    return (tracks.filter(F.col("dt_s").isNotNull() & (F.col("dt_s") >= threshold_s))
            .select("mmsi", "vessel_group", "transceiver",
                    F.col("prev_epoch").alias("start_epoch"), F.col("epoch").alias("end_epoch"),
                    F.col("prev_lat").alias("start_lat"), F.col("prev_lon").alias("start_lon"),
                    F.col("lat").alias("end_lat"), F.col("lon").alias("end_lon"),
                    F.col("prev_sog").alias("sog_before"), F.col("sog").alias("sog_after"),
                    F.col("prev_cog").alias("cog_before"), F.col("cog").alias("cog_after"),
                    "dt_s", "dist_nm", "implied_kn", F.col("cog_change").alias("course_change_deg"))
            .withColumn("gap_id", F.concat_ws("-", F.col("mmsi").cast("string"), F.col("start_epoch").cast("string")))
            .withColumn("gap_h", F.round(F.col("dt_s") / 3600.0, 3)))


def add_context(gaps, density, ports, cfg_dv: dict, stationary_sog: float):
    g = cfg_dv["density_grid_deg"]
    gaps = (gaps
            .withColumn("cell_lat", F.floor(F.col("start_lat") / g).cast("int"))
            .withColumn("cell_lon", F.floor(F.col("start_lon") / g).cast("int"))
            .withColumn("hour_epoch", (F.floor(F.col("start_epoch") / 3600) * 3600).cast("long"))
            .join(density, ["cell_lat", "cell_lon", "hour_epoch"], "left")
            .withColumn("density_start", F.coalesce(F.col("density"), F.lit(0)))
            .drop("density", "cell_lat", "cell_lon", "hour_epoch"))

    gaps = gaps.withColumn("row_id", F.col("gap_id"))
    gaps = nearest_port_distance(gaps, ports, "start_lat", "start_lon", prefix="start_")
    gaps = nearest_port_distance(gaps, ports, "end_lat", "end_lon", prefix="end_").drop("row_id")

    start_ts = F.from_unixtime("start_epoch").cast("timestamp")
    lh = local_solar_hour(start_ts, F.col("start_lon"))
    ns, ne = cfg_dv["night_start_hour"], cfg_dv["night_end_hour"]
    avg_sog = (F.coalesce("sog_before", "sog_after", F.lit(0.0)) + F.coalesce("sog_after", "sog_before", F.lit(0.0))) / 2
    gaps = (gaps
            .withColumn("start_ts", start_ts)
            .withColumn("end_ts", F.from_unixtime("end_epoch").cast("timestamp"))
            .withColumn("local_hour", F.round(lh, 2))
            .withColumn("night", ((lh >= ns) | (lh < ne)).cast("int"))
            .withColumn("avg_reported_sog", F.round(avg_sog, 2))
            .withColumn("speed_mismatch_kn", F.round(F.abs(F.coalesce("implied_kn", F.lit(0.0)) - avg_sog), 2))
            .withColumn("stationary_in_port",
                        ((F.coalesce("sog_before", F.lit(0.0)) < stationary_sog)
                         & (F.coalesce("sog_after", F.lit(0.0)) < stationary_sog)
                         & (F.col("dist_nm") < 0.5)
                         & (F.col("start_dist_to_port_nm") <= 15)).cast("int"))
            .withColumn("underway_before", (F.coalesce("sog_before", F.lit(0.0)) >= 2.0).cast("int"))
            .withColumn("event_date", F.date_format("start_ts", "yyyy-MM-dd")))
    return gaps


def add_vessel_history(gaps, registry):
    span = (registry.select("mmsi", ((F.unix_timestamp("last_seen") - F.unix_timestamp("first_seen")) / 86400.0)
                            .alias("active_days")))
    per_vessel = gaps.groupBy("mmsi").agg(F.count(F.lit(1)).alias("vessel_gap_count"))
    return (gaps.join(per_vessel, "mmsi", "left").join(F.broadcast(span), "mmsi", "left")
            .withColumn("vessel_gaps_per_day",
                        F.round(F.col("vessel_gap_count") / F.greatest(F.lit(1.0), F.col("active_days")), 3))
            .drop("active_days"))


def add_ml_features(gaps):
    return (gaps
            .withColumn("f_log_gap_h", F.log(F.col("gap_h")))
            .withColumn("f_log_dist", F.log1p(F.coalesce("dist_nm", F.lit(0.0))))
            .withColumn("f_implied_kn", F.least(F.lit(60.0), F.coalesce("implied_kn", F.lit(0.0))))
            .withColumn("f_sog_before", F.coalesce("sog_before", F.lit(0.0)))
            .withColumn("f_sog_after", F.coalesce("sog_after", F.lit(0.0)))
            .withColumn("f_speed_mismatch", F.least(F.lit(60.0), F.col("speed_mismatch_kn")))
            .withColumn("f_course_change", F.coalesce("course_change_deg", F.lit(0.0)))
            .withColumn("f_log_port_dist", F.log1p(F.coalesce("start_dist_to_port_nm", F.lit(500.0))))
            .withColumn("f_log_density", F.log1p("density_start"))
            .withColumn("f_night", F.col("night").cast("double"))
            .withColumn("f_log_gap_rate", F.log1p("vessel_gaps_per_day")))


def run(spark=None):
    cfg = load_config()
    dv = cfg["dark_vessel"]
    spark = spark or get_spark("06_ais_gaps")
    with Timer() as t:
        tracks = read_csv(spark, "vessel_tracks", TRACK_SCHEMA)
        clean = read_csv(spark, "clean_ais", CLEAN_AIS_SCHEMA)
        registry = read_csv(spark, "vessel_registry")
        ports = ports_df(spark)

        g = dv["density_grid_deg"]
        density = (clean.groupBy(F.floor(F.col("lat") / g).cast("int").alias("cell_lat"),
                                 F.floor(F.col("lon") / g).cast("int").alias("cell_lon"),
                                 (F.floor(F.col("epoch") / 3600) * 3600).cast("long").alias("hour_epoch"))
                   .agg(F.countDistinct("mmsi").alias("density")))

        gaps = extract_gaps(tracks, dv["gap_minutes_class_a"], dv["gap_minutes_class_b"])
        gaps = add_context(gaps, density, ports, dv, cfg["trajectory"]["stationary_sog_kn"])
        gaps = add_vessel_history(gaps, registry)
        gaps = add_ml_features(gaps).cache()

        write_csv(gaps.drop(*FEATURE_COLS), "ais_gaps", partition_by=["event_date"])
        write_csv(gaps.select("gap_id", "mmsi", *FEATURE_COLS), "gap_features")

        s = gaps.agg(F.count(F.lit(1)).alias("gaps"), F.countDistinct("mmsi").alias("vessels"),
                     F.sum("stationary_in_port").alias("stationary_in_port"),
                     F.sum("underway_before").alias("underway_before"),
                     F.expr("percentile_approx(gap_h, array(0.5, 0.9, 0.99))").alias("q")).first()
        by_class = {r["transceiver"]: r["count"] for r in gaps.groupBy("transceiver").count().collect()}
        gaps.unpersist()

    record_metrics(spark, "ais_gaps", {
        "gaps": s["gaps"], "vessels_with_gaps": s["vessels"],
        "stationary_in_port_gaps": s["stationary_in_port"], "underway_before_gaps": s["underway_before"],
        "gap_hours_p50_p90_p99": s["q"], "gaps_by_transceiver": by_class,
        "threshold_minutes": {"A": dv["gap_minutes_class_a"], "B": dv["gap_minutes_class_b"]},
        "elapsed_seconds": t.seconds,
    })


if __name__ == "__main__":
    run()
