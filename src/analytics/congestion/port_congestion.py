"""Stage 4 - Port Congestion Intelligence (Spark SQL / DataFrames).

processed/port_visits -> analytics/port_congestion/{hourly, daily, port_summary}

Occupancy is computed from visit *intervals* rather than from raw fixes, so a
vessel that reports only every 20-40 min is still counted in every hour it is
present:

  in_area    hours overlapping [arrival, departure]           (non pass-through)
  at_berth   hours overlapping [berth_start, berth_end]
  queue      hours overlapping the anchorage phase before berthing
             (or the whole anchorage stay for anchored-only visits)

PORT CONGESTION INDEX (PCI) - project-defined, NOT an industry standard.
  PCI = 100 * ( w_q * norm(avg queue)       + w_w * norm(avg waiting hours)
              + w_d * norm(avg dwell hours) + w_u * norm(berth utilisation) )
  norm(x) = clip((x - P5) / (P95 - P5), 0, 1) across ports (or port-days).
  Utilisation = average vessels at berth / observed capacity proxy, where the
  capacity proxy is the 95th percentile of hourly at-berth counts (real berth
  counts are not in the dataset).
"""
from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.analytics.ports import ports_df
from src.common.config import load_config
from src.common.metrics import Timer, read_metrics, record_metrics
from src.common.spark import get_spark, read_csv, write_csv

MIN_BERTHED_VISITS_FOR_PCI = 3


def _hours_of(start: str, end: str) -> F.Column:
    """Array of hour-start epochs overlapping [start, end]."""
    s = F.floor(F.col(start) / 3600) * 3600
    e = F.floor(F.col(end) / 3600) * 3600
    return F.when(F.col(start).isNotNull() & F.col(end).isNotNull(),
                  F.sequence(s.cast("long"), e.cast("long"), F.lit(3600).cast("long")))


def hourly_occupancy(spark: SparkSession, visits: DataFrame, port_ids: list[str],
                     window_start: int, window_end: int) -> DataFrame:
    v = visits.filter(F.col("visit_type") != "PASS_THROUGH")

    def count_hours(start, end, name):
        return (v.select("port_id", "mmsi", F.explode(_hours_of(start, end)).alias("hour_epoch"))
                .groupBy("port_id", "hour_epoch").agg(F.countDistinct("mmsi").alias(name)))

    in_area = count_hours("arrival_epoch", "departure_epoch", "in_area")
    at_berth = count_hours("berth_start_epoch", "berth_end_epoch", "at_berth")
    queue = count_hours("anchor_start_epoch", "anchor_end_epoch", "queue")
    arrivals = (v.filter(~F.col("censored_arrival"))
                .groupBy("port_id", (F.floor(F.col("arrival_epoch") / 3600) * 3600).alias("hour_epoch"))
                .agg(F.count(F.lit(1)).alias("arrivals")))
    departures = (v.filter(~F.col("censored_departure"))
                  .groupBy("port_id", (F.floor(F.col("departure_epoch") / 3600) * 3600).alias("hour_epoch"))
                  .agg(F.count(F.lit(1)).alias("departures")))

    h0, h1 = (window_start // 3600) * 3600, (window_end // 3600) * 3600
    grid = (spark.createDataFrame([(p,) for p in port_ids], "port_id string")
            .crossJoin(spark.range(h0, h1 + 1, 3600).withColumnRenamed("id", "hour_epoch")))
    hourly = reduce(lambda a, b: a.join(b, ["port_id", "hour_epoch"], "left"),
                    [grid, in_area, at_berth, queue, arrivals, departures])
    return (hourly.fillna(0, ["in_area", "at_berth", "queue", "arrivals", "departures"])
            .withColumn("hour_ts", F.from_unixtime("hour_epoch").cast("timestamp"))
            .withColumn("date", F.date_format("hour_ts", "yyyy-MM-dd")))


def normalise(df: DataFrame, cols: list[str], eligible: F.Column) -> DataFrame:
    """Add norm_<col> = clip((x-P5)/(P95-P5),0,1) computed over eligible rows."""
    q = df.filter(eligible).agg(*[
        F.expr(f"percentile_approx({c}, array(0.05, 0.95), 10000)").alias(c) for c in cols]).first()
    for c in cols:
        lo, hi = (q[c] or [0.0, 0.0])
        lo, hi = float(lo or 0.0), float(hi or 0.0)
        span = hi - lo
        expr = (F.lit(0.0) if span <= 0 else
                F.greatest(F.lit(0.0), F.least(F.lit(1.0), (F.coalesce(F.col(c), F.lit(lo)) - lo) / span)))
        df = df.withColumn(f"norm_{c}", F.round(expr, 4))
    return df


def pci_expr(weights: dict) -> F.Column:
    return F.round(100 * (weights["queue"] * F.col("norm_avg_queue")
                          + weights["waiting"] * F.col("norm_avg_waiting_h")
                          + weights["dwell"] * F.col("norm_avg_dwell_h")
                          + weights["utilization"] * F.col("norm_utilization")), 1)


def pci_band(col: F.Column) -> F.Column:
    return (F.when(col.isNull(), F.lit("Insufficient data"))
            .when(col <= 30, F.lit("Low")).when(col <= 60, F.lit("Moderate"))
            .when(col <= 80, F.lit("High")).otherwise(F.lit("Severe")))


def run(spark=None):
    cfg = load_config()
    cc = cfg["congestion"]
    spark = spark or get_spark("04_port_congestion")
    win = read_metrics(spark, "clean_ais")

    with Timer() as t:
        visits = read_csv(spark, "port_visits").cache()
        ports = ports_df(spark)
        port_ids = [r["port_id"] for r in ports.select("port_id").collect()]

        hourly = hourly_occupancy(spark, visits, port_ids,
                                  win["window_start_epoch"], win["window_end_epoch"]).cache()
        write_csv(hourly.orderBy("port_id", "hour_epoch"), "congestion_hourly", coalesce=1)

        # ---- per-port visit statistics -----------------------------------
        berthed = visits.filter(F.col("visit_type") == "BERTHED")
        wait_ok = berthed.filter(~F.col("censored_arrival"))
        complete = berthed.filter(F.col("complete"))

        def stats(df, col, prefix):
            return df.groupBy("port_id").agg(
                F.round(F.avg(col), 2).alias(f"avg_{prefix}"),
                F.round(F.expr(f"percentile_approx({col}, 0.5)"), 2).alias(f"median_{prefix}"),
                F.round(F.expr(f"percentile_approx({col}, 0.9)"), 2).alias(f"p90_{prefix}"))

        vcounts = visits.groupBy("port_id").agg(
            F.sum(((F.col("visit_type") != "PASS_THROUGH") & ~F.col("censored_arrival")).cast("int")).alias("arrivals"),
            F.sum(((F.col("visit_type") != "PASS_THROUGH") & ~F.col("censored_departure")).cast("int")).alias("departures"),
            F.sum((F.col("visit_type") == "BERTHED").cast("int")).alias("berthed_visits"),
            F.sum((F.col("visit_type") == "ANCHORED_ONLY").cast("int")).alias("anchored_only_visits"),
            F.sum((F.col("visit_type") == "PASS_THROUGH").cast("int")).alias("pass_through_visits"),
            F.sum(((F.col("visit_type") == "BERTHED") & F.col("complete")).cast("int")).alias("complete_berthed_visits"),
            F.countDistinct(F.when(F.col("visit_type") != "PASS_THROUGH", F.col("mmsi"))).alias("unique_vessels"))
        waited = wait_ok.groupBy("port_id").agg(
            F.round(F.avg((F.col("waiting_h") > 0.5).cast("double")) * 100, 1).alias("pct_visits_waited"))

        occ = hourly.groupBy("port_id").agg(
            F.round(F.avg("queue"), 2).alias("avg_queue"), F.max("queue").alias("peak_queue"),
            F.round(F.avg("at_berth"), 2).alias("avg_at_berth"), F.max("at_berth").alias("peak_at_berth"),
            F.round(F.avg("in_area"), 2).alias("avg_in_area"),
            F.greatest(F.lit(1.0), F.expr(f"percentile_approx(at_berth, {cc['capacity_percentile']})")
                       .cast("double")).alias("capacity_proxy"),
            F.max_by("hour_ts", "queue").alias("peak_queue_hour"))

        summary = reduce(lambda a, b: a.join(b, "port_id", "left"), [
            ports.select("port_id", "port_name", "region", "port_lat", "port_lon", "port_radius_nm",
                         "anch_lat", "anch_lon", "anch_radius_nm"),
            vcounts, occ, stats(wait_ok, "waiting_h", "waiting_h"),
            stats(complete, "dwell_h", "dwell_h"), stats(complete, "turnaround_h", "turnaround_h"), waited])
        summary = (summary
                   .fillna(0, ["arrivals", "departures", "berthed_visits", "anchored_only_visits",
                               "pass_through_visits", "complete_berthed_visits", "unique_vessels"])
                   .withColumn("utilization", F.round(F.least(F.lit(1.0),
                                                              F.col("avg_at_berth") / F.col("capacity_proxy")), 3)))
        eligible = F.col("berthed_visits") >= MIN_BERTHED_VISITS_FOR_PCI
        summary = normalise(summary, ["avg_queue", "avg_waiting_h", "avg_dwell_h", "utilization"], eligible)
        summary = (summary
                   .withColumn("pci", F.when(eligible, pci_expr(cc["pci_weights"])))
                   .withColumn("pci_band", pci_band(F.col("pci")))
                   .withColumn("pci_rank", F.when(eligible, F.dense_rank().over(
                       Window.orderBy(F.desc_nulls_last("pci"))))))
        summary = summary.cache()
        write_csv(summary, "port_summary", coalesce=1)

        # ---- daily congestion + daily PCI --------------------------------
        cap = summary.select("port_id", "capacity_proxy", F.col("avg_dwell_h").alias("_port_dwell"))
        day = lambda c: F.date_format(F.from_unixtime(F.col(c)), "yyyy-MM-dd")
        d_occ = hourly.groupBy("port_id", "date").agg(
            F.sum("arrivals").alias("arrivals"), F.sum("departures").alias("departures"),
            F.round(F.avg("queue"), 2).alias("avg_queue"), F.max("queue").alias("peak_queue"),
            F.round(F.avg("at_berth"), 2).alias("avg_at_berth"), F.max("at_berth").alias("peak_at_berth"),
            F.round(F.avg("in_area"), 2).alias("avg_in_area"))
        d_wait = wait_ok.groupBy("port_id", day("berth_start_epoch").alias("date")).agg(
            F.round(F.avg("waiting_h"), 2).alias("avg_waiting_h"))
        d_dwell = complete.groupBy("port_id", day("berth_end_epoch").alias("date")).agg(
            F.round(F.avg("dwell_h"), 2).alias("avg_dwell_h"),
            F.round(F.avg("turnaround_h"), 2).alias("avg_turnaround_h"))
        daily = (d_occ.join(d_wait, ["port_id", "date"], "left").join(d_dwell, ["port_id", "date"], "left")
                 .join(cap, "port_id", "left")
                 .withColumn("avg_waiting_h", F.coalesce("avg_waiting_h", F.lit(0.0)))
                 .withColumn("avg_dwell_h", F.coalesce("avg_dwell_h", "_port_dwell", F.lit(0.0)))
                 .withColumn("utilization", F.round(F.least(F.lit(1.0),
                                                            F.col("avg_at_berth") / F.col("capacity_proxy")), 3))
                 .drop("_port_dwell"))
        d_eligible = F.col("avg_in_area") > 0
        daily = normalise(daily, ["avg_queue", "avg_waiting_h", "avg_dwell_h", "utilization"], d_eligible)
        daily = (daily.withColumn("pci", F.when(d_eligible, pci_expr(cc["pci_weights"])))
                 .withColumn("pci_band", pci_band(F.col("pci"))))
        write_csv(daily.orderBy("port_id", "date"), "congestion_daily", coalesce=1)

        top = summary.filter(eligible).orderBy(F.desc("pci")).select("port_id", "pci").limit(5).collect()
        n_eligible = summary.filter(eligible).count()
        visits.unpersist(); hourly.unpersist(); summary.unpersist()

    record_metrics(spark, "port_congestion", {
        "ports_configured": len(port_ids),
        "ports_with_pci": n_eligible,
        "top_pci": {r["port_id"]: r["pci"] for r in top},
        "pci_weights": cc["pci_weights"],
        "elapsed_seconds": t.seconds,
    })


if __name__ == "__main__":
    run()
