"""Geospatial helpers expressed as native Spark Column expressions.

No Python UDFs are used: everything compiles to JVM expressions so it runs
efficiently on executors.
"""
from __future__ import annotations

import math

from pyspark.sql import Column
from pyspark.sql import functions as F

EARTH_RADIUS_NM = 3440.065


def _c(x) -> Column:
    """Accept a column name or a Column."""
    return F.col(x) if isinstance(x, str) else x


def haversine_nm(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Great-circle distance in nautical miles."""
    lat1, lon1, lat2, lon2 = map(_c, (lat1, lon1, lat2, lon2))
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2)
    return F.lit(2 * EARTH_RADIUS_NM) * F.asin(F.least(F.lit(1.0), F.sqrt(a)))


def bearing_deg(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Initial bearing from point 1 to point 2, 0-360 degrees."""
    lat1, lon1, lat2, lon2 = map(_c, (lat1, lon1, lat2, lon2))
    phi1, phi2 = F.radians(lat1), F.radians(lat2)
    dlon = F.radians(lon2 - lon1)
    y = F.sin(dlon) * F.cos(phi2)
    x = F.cos(phi1) * F.sin(phi2) - F.sin(phi1) * F.cos(phi2) * F.cos(dlon)
    return (F.degrees(F.atan2(y, x)) + 360) % 360


def angle_diff_deg(a: Column, b: Column) -> Column:
    """Smallest absolute difference between two angles (0-180)."""
    a, b = _c(a), _c(b)
    d = F.abs(a - b) % 360
    return F.when(d > 180, 360 - d).otherwise(d)


def local_solar_hour(ts: Column, lon: Column) -> Column:
    """Approximate local solar hour from a UTC timestamp and longitude."""
    utc_hour = F.hour(ts) + F.minute(ts) / 60.0
    return F.pmod(utc_hour + lon / 15.0, F.lit(24.0))


# Pure-python versions (used for tests and port lookups in the driver)
def haversine_nm_py(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    return 2 * EARTH_RADIUS_NM * math.asin(min(1.0, math.sqrt(a)))
