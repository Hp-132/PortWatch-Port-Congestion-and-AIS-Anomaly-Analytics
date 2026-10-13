"""Stage 5 - Vessel Turnaround Intelligence (Spark SQL).

processed/port_visits + processed/vessel_registry
    -> analytics/turnaround/visits  (enriched, non pass-through visits)
    -> analytics/turnaround/stats   (per port x vessel group, per port, overall)

Phases per visit: arrival -> anchorage (waiting) -> berth (dwell, used as the
operations proxy - AIS cannot distinguish cargo handling from idle berth
time) -> departure. Turnaround = departure - arrival.

Censoring: the dataset covers one week, so visits already in port at the
start, or still in port at the end, have unknown true arrival/departure.
Waiting statistics use visits with an observed arrival; dwell and turnaround
use only complete visits (both ends observed). This biases averages toward
shorter stays, which is documented as a limitation.
"""
from __future__ import annotations

from pyspark.sql import functions as F

from src.analytics.ports import ports_df
from src.common.metrics import Timer, record_metrics
from src.common.spark import get_spark, read_csv, write_csv


def run(spark=None):
    spark = spark or get_spark("05_turnaround")
    with Timer() as t:
        visits = read_csv(spark, "port_visits")
        registry = read_csv(spark, "vessel_registry").select(
            "mmsi", "vessel_name", "imo", "vessel_type", "vessel_group", "length", "width", "max_draft")
        ports = ports_df(spark).select("port_id", "port_name")

        enriched = (visits.filter(F.col("visit_type") != "PASS_THROUGH")
                    .join(F.broadcast(ports), "port_id", "left")
                    .join(registry, "mmsi", "left")
                    .withColumn("waited", F.col("waiting_h") > 0.5)
                    .select("visit_id", "mmsi", "vessel_name", "imo", "vessel_group", "length", "max_draft",
                            "port_id", "port_name", "visit_type", "arrival_ts", "anchor_start_ts",
                            "anchor_end_ts", "berth_start_ts", "berth_end_ts", "departure_ts",
                            "arrival_epoch", "departure_epoch", "waiting_h", "dwell_h", "turnaround_h",
                            "anchor_only_h", "waited", "censored_arrival", "censored_departure",
                            "complete", "fixes")
                    .cache())
        write_csv(enriched, "turnaround_visits", coalesce=2)

        enriched.createOrReplaceTempView("visits")
        # Spark SQL with GROUPING SETS: per port x group, per port, overall.
        stats = spark.sql("""
            SELECT
              COALESCE(port_id, 'ALL')      AS port_id,
              COALESCE(vessel_group, 'ALL') AS vessel_group,
              COUNT(*)                                                    AS visits,
              SUM(CASE WHEN visit_type = 'BERTHED' THEN 1 ELSE 0 END)     AS berthed_visits,
              SUM(CASE WHEN visit_type = 'ANCHORED_ONLY' THEN 1 ELSE 0 END) AS anchored_only_visits,
              SUM(CASE WHEN complete AND visit_type = 'BERTHED' THEN 1 ELSE 0 END) AS complete_visits,
              ROUND(AVG(CASE WHEN visit_type='BERTHED' AND NOT censored_arrival THEN waiting_h END), 2) AS avg_waiting_h,
              ROUND(PERCENTILE_APPROX(CASE WHEN visit_type='BERTHED' AND NOT censored_arrival THEN waiting_h END, 0.5), 2) AS median_waiting_h,
              ROUND(AVG(CASE WHEN visit_type='BERTHED' AND NOT censored_arrival THEN CAST(waited AS INT) END) * 100, 1) AS pct_waited,
              ROUND(AVG(CASE WHEN visit_type='BERTHED' AND complete THEN dwell_h END), 2) AS avg_dwell_h,
              ROUND(PERCENTILE_APPROX(CASE WHEN visit_type='BERTHED' AND complete THEN dwell_h END, 0.5), 2) AS median_dwell_h,
              ROUND(AVG(CASE WHEN visit_type='BERTHED' AND complete THEN turnaround_h END), 2) AS avg_turnaround_h,
              ROUND(PERCENTILE_APPROX(CASE WHEN visit_type='BERTHED' AND complete THEN turnaround_h END, 0.5), 2) AS median_turnaround_h,
              ROUND(PERCENTILE_APPROX(CASE WHEN visit_type='BERTHED' AND complete THEN turnaround_h END, 0.9), 2) AS p90_turnaround_h,
              ROUND(AVG(CASE WHEN visit_type='ANCHORED_ONLY' AND NOT censored_arrival THEN anchor_only_h END), 2) AS avg_anchor_only_h
            FROM visits
            GROUP BY GROUPING SETS ((port_id, vessel_group), (port_id), (vessel_group), ())
        """)
        write_csv(stats, "turnaround_stats", coalesce=1)

        overall = stats.filter("port_id = 'ALL' AND vessel_group = 'ALL'").first().asDict()
        enriched.unpersist()

    record_metrics(spark, "turnaround", {
        "visits": overall["visits"], "berthed_visits": overall["berthed_visits"],
        "complete_visits": overall["complete_visits"],
        "avg_waiting_h": overall["avg_waiting_h"], "avg_dwell_h": overall["avg_dwell_h"],
        "avg_turnaround_h": overall["avg_turnaround_h"], "elapsed_seconds": t.seconds,
    })


if __name__ == "__main__":
    run()
