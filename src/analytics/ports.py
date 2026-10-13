"""Port reference data and zone assignment.

Each port in ``config/ports.csv`` has two circular zones (project-defined
geofences, approximations of real port limits):

  PORT       circle around the terminal / berth area   (port_radius_nm)
  ANCHORAGE  circle around the main waiting anchorage  (anch_radius_nm)

Every commercial AIS fix inside a zone is assigned an operational state:

  BERTH   inside PORT zone and moored (status 5) or stationary (not anchored)
  ANCHOR  reported 'at anchor' (status 1), or stationary inside ANCHORAGE zone
  MOVING  anything else (manoeuvring, transiting the zone)
"""
from __future__ import annotations

import csv

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.common.config import PROJECT_ROOT, load_config
from src.common.geo import haversine_nm

PORT_SCHEMA = StructType([
    StructField("port_id", StringType()), StructField("port_name", StringType()),
    StructField("region", StringType()),
    StructField("port_lat", DoubleType()), StructField("port_lon", DoubleType()),
    StructField("port_radius_nm", DoubleType()),
    StructField("anch_lat", DoubleType()), StructField("anch_lon", DoubleType()),
    StructField("anch_radius_nm", DoubleType()),
])


def load_ports() -> list[dict]:
    path = PROJECT_ROOT / load_config()["ports"]["reference_file"]
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    for r in rows:
        for k in ("port_lat", "port_lon", "port_radius_nm", "anch_lat", "anch_lon", "anch_radius_nm"):
            r[k] = float(r[k])
    return rows


def ports_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([tuple(p[f.name] for f in PORT_SCHEMA.fields) for p in load_ports()],
                                 PORT_SCHEMA)


def assign_zones(points: DataFrame, ports: DataFrame, stationary_sog: float) -> DataFrame:
    """Attach the nearest port zone and operational state to AIS fixes.

    ``points`` needs mmsi, epoch, lat, lon, sog, status. Fixes outside every
    zone are dropped. Implemented as a broadcast join (ports table is tiny)
    with a bounding-box pre-filter, then exact haversine distances.
    """
    # Bounding box around each port covering both zones
    p = (ports
         .withColumn("min_lat", F.least(F.col("port_lat") - F.col("port_radius_nm") / 60,
                                        F.col("anch_lat") - F.col("anch_radius_nm") / 60))
         .withColumn("max_lat", F.greatest(F.col("port_lat") + F.col("port_radius_nm") / 60,
                                           F.col("anch_lat") + F.col("anch_radius_nm") / 60))
         .withColumn("lon_scale", F.cos(F.radians("port_lat")) * 60)
         .withColumn("min_lon", F.least(F.col("port_lon") - F.col("port_radius_nm") / F.col("lon_scale"),
                                        F.col("anch_lon") - F.col("anch_radius_nm") / F.col("lon_scale")))
         .withColumn("max_lon", F.greatest(F.col("port_lon") + F.col("port_radius_nm") / F.col("lon_scale"),
                                           F.col("anch_lon") + F.col("anch_radius_nm") / F.col("lon_scale"))))

    cond = ((points["lat"] >= p["min_lat"]) & (points["lat"] <= p["max_lat"])
            & (points["lon"] >= p["min_lon"]) & (points["lon"] <= p["max_lon"]))
    j = (points.join(F.broadcast(p), cond, "inner")
         .withColumn("d_port_nm", haversine_nm("lat", "lon", "port_lat", "port_lon"))
         .withColumn("d_anch_nm", haversine_nm("lat", "lon", "anch_lat", "anch_lon"))
         .withColumn("zone", F.when(F.col("d_port_nm") <= F.col("port_radius_nm"), F.lit("PORT"))
                     .when(F.col("d_anch_nm") <= F.col("anch_radius_nm"), F.lit("ANCHORAGE")))
         .filter(F.col("zone").isNotNull()))

    # If zones of two ports overlap keep the PORT zone first, then the nearest
    # zone; a shared anchorage goes to the nearest port centre (deterministic).
    w = Window.partitionBy("mmsi", "epoch", "lat", "lon").orderBy(
        F.when(F.col("zone") == "PORT", 0).otherwise(1), F.least("d_port_nm", "d_anch_nm"),
        "d_port_nm", "port_id")
    j = j.withColumn("_rk", F.row_number().over(w)).filter("_rk = 1").drop("_rk")

    # Declared navigational status wins over geometry: river ports (New
    # Orleans, Baton Rouge, Houston Ship Channel) have terminals far from the
    # port centre, so "moored" must mean BERTH in either zone. Without a
    # declared status, a stationary vessel is at berth in the PORT zone and
    # at anchor in the ANCHORAGE zone.
    stationary = F.col("sog") < stationary_sog
    state = (F.when(F.col("status") == 5, F.lit("BERTH"))
             .when(F.col("status") == 1, F.lit("ANCHOR"))
             .when((F.col("zone") == "PORT") & stationary, F.lit("BERTH"))
             .when((F.col("zone") == "ANCHORAGE") & stationary, F.lit("ANCHOR"))
             .otherwise(F.lit("MOVING")))
    return (j.withColumn("state", state)
            .withColumn("d_port_nm", F.round("d_port_nm", 3))
            .select(*points.columns, "port_id", "zone", "state", "d_port_nm"))


def nearest_port_distance(df: DataFrame, ports: DataFrame, lat_col: str, lon_col: str,
                          prefix: str = "") -> DataFrame:
    """Add nearest port id and distance (nm) for each row (broadcast cross join).

    ``df`` must have a unique ``row_id`` column.
    """
    j = (df.select("row_id", lat_col, lon_col)
         .crossJoin(F.broadcast(ports.select("port_id", "port_lat", "port_lon")))
         .withColumn("_d", haversine_nm(F.col(lat_col), F.col(lon_col), F.col("port_lat"), F.col("port_lon")))
         .groupBy("row_id")
         .agg(F.min_by("port_id", "_d").alias(f"{prefix}nearest_port"),
              F.round(F.min("_d"), 2).alias(f"{prefix}dist_to_port_nm")))
    return df.join(j, "row_id", "left")
