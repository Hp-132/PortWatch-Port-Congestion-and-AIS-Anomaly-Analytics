from pyspark.sql import functions as F

from src.analytics.congestion.port_congestion import hourly_occupancy, normalise, pci_band

H = 3600
VCOLS = ("port_id string, mmsi long, visit_type string, arrival_epoch long, departure_epoch long, "
         "berth_start_epoch long, berth_end_epoch long, anchor_start_epoch long, anchor_end_epoch long, "
         "censored_arrival boolean, censored_departure boolean")


def test_hourly_occupancy_counts_vessels_from_intervals(spark):
    visits = spark.createDataFrame([
        # anchored 10:00-12:30, berthed 13:00-15:00, left 15:30
        ("P", 1, "BERTHED", 10 * H, 15 * H + 1800, 13 * H, 15 * H, 10 * H, 12 * H + 1800, False, False),
        ("P", 2, "PASS_THROUGH", 11 * H, 11 * H + 600, None, None, None, None, False, False),
    ], VCOLS)
    h = {r["hour_epoch"] // H: r for r in hourly_occupancy(spark, visits, ["P", "Q"], 0, 24 * H)
         .filter("port_id = 'P'").collect()}
    assert h[11]["queue"] == 1 and h[11]["at_berth"] == 0
    assert h[14]["at_berth"] == 1 and h[14]["queue"] == 0
    assert h[10]["arrivals"] == 1 and h[15]["departures"] == 1
    assert h[11]["in_area"] == 1                     # pass-through vessel not counted
    assert h[20]["in_area"] == 0                     # zero-filled grid


def test_normalise_clips_to_unit_interval(spark):
    df = spark.createDataFrame([(float(i),) for i in range(101)], "x double")
    out = normalise(df, ["x"], F.lit(True)).agg(F.min("norm_x"), F.max("norm_x")).first()
    assert out[0] == 0.0 and out[1] == 1.0


def test_pci_band_labels(spark):
    df = spark.createDataFrame([(None,), (10.0,), (45.0,), (70.0,), (95.0,)], "p double")
    got = [r["b"] for r in df.select(pci_band(F.col("p")).alias("b")).collect()]
    assert got == ["Insufficient data", "Low", "Moderate", "High", "Severe"]
