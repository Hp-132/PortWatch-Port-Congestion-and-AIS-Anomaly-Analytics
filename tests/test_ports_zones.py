from src.analytics.ports import assign_zones

PORTS = "port_id string, port_lat double, port_lon double, port_radius_nm double, " \
        "anch_lat double, anch_lon double, anch_radius_nm double"
POINTS = "mmsi long, epoch long, lat double, lon double, sog double, status int"


def test_moored_status_is_berth_even_in_anchorage_ring(spark):
    ports = spark.createDataFrame([("P", 30.0, -90.0, 5.0, 30.0, -90.0, 15.0)], PORTS)
    pts = spark.createDataFrame([
        (1, 0, 30.0, -90.0, 0.0, None),     # stationary in PORT zone          -> BERTH
        (2, 0, 30.15, -90.0, 0.0, 5),       # moored, 9 nm out (ANCHORAGE ring) -> BERTH
        (3, 0, 30.15, -90.0, 0.0, None),    # stationary in ANCHORAGE ring      -> ANCHOR
        (4, 0, 30.0, -90.0, 0.0, 1),        # declared anchored inside PORT     -> ANCHOR
        (5, 0, 30.0, -90.0, 8.0, 0),        # under way                         -> MOVING
    ], POINTS)
    got = {r["mmsi"]: r["state"] for r in assign_zones(pts, ports, 0.5).collect()}
    assert got == {1: "BERTH", 2: "BERTH", 3: "ANCHOR", 4: "ANCHOR", 5: "MOVING"}
