"""Data-driven port hotspot discovery (Spark).

The AIS dataset contains no port reference. To ground the curated port list
in ``config/ports.csv`` we find where commercial vessels (Cargo, Tanker)
actually stop: stationary fixes are snapped to a 0.05 degree grid and ranked
by the number of distinct vessels. Moored (status 5) and anchored
(status 1) shares help separate berth areas from anchorages.

Usage::  python -m src.ingestion.discover_port_hotspots [--top 80]

Output: output/reports/port_hotspots.csv and a check of each configured port
(how many stationary commercial vessels fall inside its berth/anchorage
zones), written to output/reports/port_reference_check.csv.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from src.common.config import PROJECT_ROOT, load_config
from src.common.geo import haversine_nm
from src.common.schemas import CLEAN_AIS_SCHEMA
from src.common.spark import get_spark, read_csv
from src.analytics.ports import load_ports


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=80)
    ap.add_argument("--grid", type=float, default=0.05)
    args = ap.parse_args()
    cfg = load_config()
    spark = get_spark("port_hotspot_discovery")
    g = args.grid

    clean = read_csv(spark, "clean_ais", CLEAN_AIS_SCHEMA)
    stationary = (clean
                  .filter(F.col("vessel_group").isin(cfg["ports"]["commercial_groups"]))
                  .filter((F.col("sog") < cfg["trajectory"]["stationary_sog_kn"]) | F.col("status").isin(1, 5))
                  .cache())

    hot = (stationary
           .withColumn("cell_lat", F.round(F.floor(F.col("lat") / g) * g + g / 2, 3))
           .withColumn("cell_lon", F.round(F.floor(F.col("lon") / g) * g + g / 2, 3))
           .groupBy("cell_lat", "cell_lon")
           .agg(F.countDistinct("mmsi").alias("vessels"),
                F.count(F.lit(1)).alias("fixes"),
                F.round(F.avg((F.col("status") == 5).cast("int")), 3).alias("moored_share"),
                F.round(F.avg((F.col("status") == 1).cast("int")), 3).alias("anchored_share"))
           .orderBy(F.desc("vessels")).limit(args.top))

    out = PROJECT_ROOT / "output" / "reports"
    out.mkdir(parents=True, exist_ok=True)
    hot.toPandas().to_csv(out / "port_hotspots.csv", index=False)
    print(hot.show(args.top, truncate=False))

    # Check coverage of the configured ports
    ports = spark.createDataFrame(load_ports())
    joined = (stationary.crossJoin(F.broadcast(ports))
              .withColumn("d_port", haversine_nm("lat", "lon", "port_lat", "port_lon"))
              .withColumn("d_anch", haversine_nm("lat", "lon", "anch_lat", "anch_lon")))
    check = (joined.groupBy("port_id", "port_name")
             .agg(F.countDistinct(F.when(F.col("d_port") <= F.col("port_radius_nm"), F.col("mmsi"))).alias("vessels_in_port_zone"),
                  F.countDistinct(F.when((F.col("d_anch") <= F.col("anch_radius_nm"))
                                         & (F.col("d_port") > F.col("port_radius_nm")), F.col("mmsi"))).alias("vessels_in_anchorage_zone"),
                  F.countDistinct(F.when((F.col("d_port") <= F.col("port_radius_nm")) & (F.col("status") == 5),
                                         F.col("mmsi"))).alias("moored_in_port_zone"),
                  F.countDistinct(F.when(F.col("status") == 1, F.when(
                      (F.col("d_port") <= F.col("port_radius_nm")) | (F.col("d_anch") <= F.col("anch_radius_nm")),
                      F.col("mmsi")))).alias("anchored_in_zones"))
             .orderBy(F.desc("vessels_in_port_zone")))
    check.toPandas().to_csv(out / "port_reference_check.csv", index=False)
    check.show(100, truncate=False)


if __name__ == "__main__":
    main()
