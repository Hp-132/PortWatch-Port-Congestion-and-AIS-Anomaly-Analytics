import json

from src.analytics.dark_vessel.visibility import find_neighbours, reporting_during, summarise

GAPS = ("gap_id string, mmsi long, start_epoch long, end_epoch long, "
        "start_lat double, start_lon double, end_lat double, end_lon double")
POINTS = "mmsi long, epoch long, lat double, lon double, sog double, vessel_group string"
H = 3600


def _scenario(spark):
    # Vessel 1 silent from t=10h to t=16h at (30.0, -90.0) -> (30.1, -90.0)
    gaps = spark.createDataFrame([("g1", 1, 10 * H, 16 * H, 30.0, -90.0, 30.1, -90.0)], GAPS)
    rows = [(1, 10 * H - 900, 30.0, -90.0, 8.0, "Cargo"), (1, 16 * H + 900, 30.1, -90.0, 8.0, "Cargo")]
    # vessel 2: 3 nm away at gap start, keeps reporting near the position every 15 min during the gap
    rows += [(2, 10 * H + i * 900, 30.05, -90.0, 0.0, "Tanker") for i in range(-2, 26)]
    # vessel 3: near the end position only at gap end, reports far away during the gap
    rows += [(3, 16 * H, 30.12, -90.0, 10.0, "Cargo"), (3, 13 * H, 31.5, -90.0, 10.0, "Cargo")]
    # vessel 4: 40 nm away -> not a neighbour
    rows += [(4, 10 * H, 30.67, -90.0, 5.0, "Fishing")]
    # vessel 5: close in space but 3 h before the gap -> outside the time window
    rows += [(5, 7 * H, 30.0, -90.01, 5.0, "Cargo")]
    return gaps, spark.createDataFrame(rows, POINTS)


def test_neighbours_respect_radius_and_time_window(spark):
    gaps, pts = _scenario(spark)
    nb = {r["nb_mmsi"]: r for r in find_neighbours(gaps, pts, 10.0, 1800, 0.2).collect()}
    assert set(nb) == {2, 3}
    assert nb[2]["side_start"] == 1 and nb[3]["side_end"] == 1 and nb[3]["side_start"] == 0
    assert 2.9 < nb[2]["min_dist_nm"] < 3.1


def test_coverage_observed_when_neighbour_reports_near_position_during_gap(spark):
    gaps, pts = _scenario(spark)
    nb = find_neighbours(gaps, pts, 10.0, 1800, 0.2)
    during = {r["nb_mmsi"]: r for r in reporting_during(gaps, nb, pts, 10.0, 7200).collect()}
    assert during[2]["reports_near_during"] > 20
    assert during[3]["reports_during"] == 1 and during[3]["reports_near_during"] == 0
    (row,) = summarise(gaps, nb, reporting_during(gaps, nb, pts, 10.0, 7200), pts, 12, 7200).collect()
    assert row["visibility"] == "coverage_observed"
    assert row["neighbours_total"] == 2 and row["neighbours_near_during"] == 1
    shown = json.loads(row["neighbours_json"])
    assert [n["nb_mmsi"] for n in shown] == [3, 2]          # closest first (3 is 1.2 nm from the end point)
    assert json.loads(row["target_timeline_json"]) == [10 * H - 900, 16 * H + 900]


def test_isolated_when_nobody_nearby(spark):
    gaps = spark.createDataFrame([("g9", 9, 10 * H, 12 * H, 10.0, -40.0, 10.5, -40.0)], GAPS)
    pts = spark.createDataFrame([(9, 10 * H - 600, 10.0, -40.0, 12.0, "Cargo")], POINTS)
    nb = find_neighbours(gaps, pts, 10.0, 1800, 0.2)
    (row,) = summarise(gaps, nb, reporting_during(gaps, nb, pts, 10.0, 7200), pts, 12, 7200).collect()
    assert row["visibility"] == "isolated" and row["neighbours_total"] == 0
