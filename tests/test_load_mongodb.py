from pyspark.sql import functions as F

from src.database.load_mongodb import _iso_timestamps


def test_dates_and_timestamps_become_bson_safe_strings(spark):
    # Parse from strings (as the pipeline does from CSV) so the host timezone
    # does not leak in through Python datetime conversion.
    df = (spark.createDataFrame([("CAVAN", "2025-01-01", "2025-01-01 06:30:00")], "port_id string, d string, t string")
          .select("port_id", F.to_date("d").alias("date"), F.to_timestamp("t").alias("hour_ts")))
    row = _iso_timestamps(df).first().asDict()
    assert row["date"] == "2025-01-01"
    assert row["hour_ts"] == "2025-01-01T06:30:00Z"
    assert row["port_id"] == "CAVAN"
