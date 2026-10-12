from pyspark.sql import functions as F

from src.common.geo import angle_diff_deg, haversine_nm, haversine_nm_py
from src.preprocessing.trajectories import add_segment_features


def test_haversine_one_degree_latitude_is_60nm():
    assert abs(haversine_nm_py(0, 0, 1, 0) - 60.04) < 0.05


def test_haversine_spark_matches_python(spark):
    df = spark.createDataFrame([(33.74, -118.23, 37.80, -122.31)], "a double, b double, c double, d double")
    got = df.select(haversine_nm(F.col("a"), F.col("b"), F.col("c"), F.col("d")).alias("d")).first()["d"]
    assert abs(got - haversine_nm_py(33.74, -118.23, 37.80, -122.31)) < 1e-6


def test_angle_diff_wraps(spark):
    df = spark.createDataFrame([(350.0, 10.0), (90.0, 270.0)], "a double, b double")
    vals = [r["d"] for r in df.select(angle_diff_deg(F.col("a"), F.col("b")).alias("d")).collect()]
    assert vals == [20.0, 180.0]


def test_segment_features_per_vessel_ordered(spark):
    rows = [  # mmsi, epoch, lat, lon, sog, cog  (out of order on purpose)
        (1, 3600, 0.1, 0.0, 6.0, 0.0),
        (1, 0, 0.0, 0.0, 6.0, 0.0),
        (2, 0, 10.0, 10.0, 0.0, 90.0),
    ]
    df = spark.createDataFrame(rows, "mmsi long, epoch long, lat double, lon double, sog double, cog double")
    out = {(r["mmsi"], r["epoch"]): r for r in add_segment_features(df).collect()}
    seg = out[(1, 3600)]
    assert seg["dt_s"] == 3600
    assert abs(seg["dist_nm"] - 6.0) < 0.05          # 0.1 deg latitude ~ 6 nm
    assert abs(seg["implied_kn"] - 6.0) < 0.05
    assert abs(seg["move_bearing"] - 0.0) < 0.5
    assert out[(1, 0)]["dt_s"] is None                # first fix has no previous
    assert out[(2, 0)]["prev_lat"] is None            # vessels are not mixed
