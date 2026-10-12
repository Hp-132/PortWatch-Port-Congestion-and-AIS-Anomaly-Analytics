"""Explicit Spark schemas and AIS code tables.

Explicit schemas avoid a full inference pass over ~48M rows and make column
types stable between local runs and the cluster.
"""
from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

RAW_AIS_SCHEMA = StructType([
    StructField("mmsi", LongType()),
    StructField("base_date_time", StringType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("sog", DoubleType()),
    StructField("cog", DoubleType()),
    StructField("heading", DoubleType()),
    StructField("vessel_name", StringType()),
    StructField("imo", StringType()),
    StructField("call_sign", StringType()),
    StructField("vessel_type", IntegerType()),
    StructField("status", IntegerType()),
    StructField("length", DoubleType()),
    StructField("width", DoubleType()),
    StructField("draft", DoubleType()),
    StructField("cargo", IntegerType()),
    StructField("transceiver", StringType()),
])

# Output of src/preprocessing/clean_ais.py (partition column event_date is
# appended by Spark when reading the partitioned folder).
CLEAN_AIS_SCHEMA = StructType([
    StructField("mmsi", LongType()),
    StructField("ts", TimestampType()),
    StructField("epoch", LongType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("sog", DoubleType()),
    StructField("cog", DoubleType()),
    StructField("heading", DoubleType()),
    StructField("status", IntegerType()),
    StructField("vessel_group", StringType()),
    StructField("transceiver", StringType()),
    StructField("event_date", StringType()),
])

TRACK_SCHEMA = StructType(CLEAN_AIS_SCHEMA.fields[:-1] + [
    StructField("prev_epoch", LongType()),
    StructField("prev_lat", DoubleType()),
    StructField("prev_lon", DoubleType()),
    StructField("prev_sog", DoubleType()),
    StructField("prev_cog", DoubleType()),
    StructField("dt_s", LongType()),
    StructField("dist_nm", DoubleType()),
    StructField("implied_kn", DoubleType()),
    StructField("cog_change", DoubleType()),
    StructField("move_bearing", DoubleType()),
    StructField("event_date", StringType()),
])

# ITU-R M.1371 navigational status codes
NAV_STATUS = {
    0: "Under way using engine", 1: "At anchor", 2: "Not under command",
    3: "Restricted manoeuvrability", 4: "Constrained by draught", 5: "Moored",
    6: "Aground", 7: "Engaged in fishing", 8: "Under way sailing",
    11: "Towing astern (regional)", 12: "Pushing ahead/towing alongside (regional)",
    14: "AIS-SART active", 15: "Undefined",
}

# (low, high, group) ranges for ITU ship-type codes, plus MarineCadastre
# extended codes (1001-1025).
_TYPE_RANGES = [
    (30, 30, "Fishing"), (31, 32, "Towing"), (33, 33, "Dredging"),
    (35, 35, "Military"), (36, 36, "Sailing"), (37, 37, "Pleasure Craft"),
    (40, 49, "High Speed Craft"), (50, 50, "Port Service"),
    (51, 51, "Search and Rescue"), (52, 52, "Tug"), (53, 54, "Port Service"),
    (55, 55, "Law Enforcement"), (56, 59, "Port Service"),
    (60, 69, "Passenger"), (70, 79, "Cargo"), (80, 89, "Tanker"),
    (1001, 1002, "Fishing"), (1003, 1004, "Cargo"), (1012, 1015, "Passenger"),
    (1016, 1016, "Cargo"), (1017, 1017, "Tanker"), (1019, 1019, "Pleasure Craft"),
    (1023, 1023, "Towing"), (1024, 1024, "Tanker"), (1025, 1025, "Tug"),
]


def vessel_group_expr(code: Column) -> Column:
    """Map ITU ship-type code to a readable vessel group."""
    expr = F.when(code.isNull(), F.lit("Unknown"))
    for lo, hi, name in _TYPE_RANGES:
        expr = expr.when(code.between(lo, hi), F.lit(name))
    return expr.otherwise(F.lit("Other"))


def vessel_group_py(code) -> str:
    if code is None:
        return "Unknown"
    for lo, hi, name in _TYPE_RANGES:
        if lo <= code <= hi:
            return name
    return "Other"
