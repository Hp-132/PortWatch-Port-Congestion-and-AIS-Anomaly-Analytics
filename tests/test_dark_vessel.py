from pyspark.sql import functions as F

from src.analytics.dark_vessel.gaps import extract_gaps
from src.analytics.dark_vessel.identity import classify_events, detect_events
from src.analytics.dark_vessel.risk_scoring import risk_level, score
from src.preprocessing.trajectories import add_segment_features

LEVELS = {"Normal": 30, "Watch": 60, "Suspicious": 80, "High Priority": 100}
ID_CFG = {"max_dt_minutes": 30, "min_jump_nm": 2.0, "sog_mismatch_factor": 3.0,
          "implausible_speed_kn": {"default": 35}}


def _tracks(spark, rows):
    df = spark.createDataFrame(
        rows, "mmsi long, epoch long, lat double, lon double, sog double, cog double, transceiver string")
    return add_segment_features(df.withColumn("vessel_group", F.lit("Cargo")))


def test_gap_threshold_depends_on_transceiver_class(spark):
    rows = [(1, 0, 30.0, -80.0, 10.0, 0.0, "A"), (1, 90 * 60, 30.2, -80.0, 10.0, 0.0, "A"),   # 90 min A -> gap
            (2, 0, 31.0, -80.0, 5.0, 0.0, "B"), (2, 90 * 60, 31.1, -80.0, 5.0, 0.0, "B")]     # 90 min B -> no gap
    gaps = extract_gaps(_tracks(spark, rows), 60, 120).collect()
    assert [g["mmsi"] for g in gaps] == [1]
    assert gaps[0]["gap_h"] == 1.5


def test_risk_level_boundaries(spark):
    df = spark.createDataFrame([(0,), (30,), (31,), (60,), (61,), (80,), (81,), (100,)], "s int")
    got = [r["l"] for r in df.select(risk_level(F.col("s"), LEVELS).alias("l")).collect()]
    assert got == ["Normal", "Normal", "Watch", "Watch", "Suspicious", "Suspicious",
                   "High Priority", "High Priority"]


def _gap(gap_h, sog_before, dist, implied, density, port_dist, stationary, ml):
    return (gap_h, sog_before, sog_before, dist, implied, abs(implied - sog_before), 10.0,
            float(sog_before), density, port_dist, 0, 0.2, stationary, ml)


def test_underway_long_gap_scores_above_stationary_port_gap(spark):
    cols = ("gap_h double, sog_before double, sog_after double, dist_nm double, implied_kn double, "
            "speed_mismatch_kn double, course_change_deg double, avg_reported_sog double, density_start long, "
            "start_dist_to_port_nm double, night int, vessel_gaps_per_day double, stationary_in_port int, "
            "ml_score double")
    df = spark.createDataFrame([
        _gap(20.0, 12.0, 60.0, 3.0, 40, 40.0, 0, 70.0),     # underway, long, dense area, speed mismatch
        _gap(1.2, 0.0, 0.01, 0.0, 40, 0.5, 1, 70.0),        # moored in port
    ], cols)
    out = score(df, density_p95=50, rule_w=0.6, ml_w=0.4, levels=LEVELS).collect()
    assert out[0]["risk_score"] > out[1]["risk_score"]
    assert out[1]["risk_level"] == "Normal"
    assert "stationary in port" in out[1]["explanation"]
    assert 0 <= out[0]["risk_score"] <= 100


def test_identity_isolated_outlier_vs_track_conflict(spark):
    base = [(9, 0, 30.0, -80.0, 10.0, 0.0, "A"), (9, 60, 30.0, -80.0, 10.0, 0.0, "A")]
    # single bad fix 100 nm away, then back on track -> isolated outlier
    outlier = base + [(9, 120, 31.7, -80.0, 10.0, 0.0, "A"), (9, 180, 30.01, -80.0, 10.0, 0.0, "A")]
    # alternating between two distant positions -> track conflict
    conflict = [(7, 0, 30.0, -80.0, 10.0, 0.0, "A"), (7, 60, 31.0, -80.0, 10.0, 0.0, "A"),
                (7, 120, 30.0, -80.0, 10.0, 0.0, "A"), (7, 180, 31.0, -80.0, 10.0, 0.0, "A"),
                (7, 240, 31.0, -80.0, 10.0, 0.0, "A")]
    tracks = _tracks(spark, outlier + conflict)
    ev = classify_events(tracks, detect_events(tracks, ID_CFG), 2.0).collect()
    by = {}
    for e in ev:
        by.setdefault(e["mmsi"], []).append(e["event_type"])
    assert by[9] == ["isolated_outlier", "isolated_outlier"]    # jump out + jump back
    assert set(by[7]) == {"track_conflict"} and len(by[7]) == 3
