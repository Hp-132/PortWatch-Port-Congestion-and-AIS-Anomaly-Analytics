"""Stage 11 - AIS proximity / visibility events around elevated AIS gaps (Spark).

The dataset contains AIS *position reports* only: there is no radio message
content and no vessel-to-vessel "handshake". What the data does support is an
AIS reporting/observation interaction: which other vessels were received near
the silent vessel just before it stopped reporting and just after it resumed,
and whether those neighbours kept being received in the same area *during*
the gap.

That last point is the useful one for analysts: if nearby vessels continued to
be received within ``radius_nm`` of the last known position throughout the
gap, the receiving infrastructure was evidently covering that area, so a
coverage hole is a less likely explanation for the silence. If no neighbour
was received there, coverage cannot be confirmed either way. Neither outcome
proves intent; the label only describes what was observed.

analytics/dark_vessel/gap_risk (score > threshold) + output/vessel_track_points
    -> analytics/dark_vessel/ais_visibility   (one row per elevated gap;
                                               neighbour list as JSON text,
                                               because the lake is CSV-only)
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.common.config import load_config
from src.common.geo import haversine_nm
from src.common.metrics import Timer, record_metrics
from src.common.spark import get_spark, read_csv, write_csv

LABELS = {
    "coverage_observed": "Nearby vessels continued to be received near the last known position during the gap",
    "coverage_unconfirmed": "Nearby vessels were observed before/after the gap, but none was received near the position during it",
    "isolated": "No other vessel was observed within the search radius before or after the gap",
}


def _cells(lat: str, lon: str, cell_deg: float):
    return (F.floor(F.col(lat) / cell_deg).cast("int"), F.floor(F.col(lon) / cell_deg).cast("int"))


def find_neighbours(gaps: DataFrame, points: DataFrame, radius_nm: float, window_s: int,
                    cell_deg: float) -> DataFrame:
    """Vessels received within ``radius_nm`` of the gap start (end) position
    within +-``window_s`` of the gap start (end) time.

    gaps:   gap_id, mmsi, start_epoch, end_epoch, start_lat, start_lon, end_lat, end_lon
    points: mmsi, epoch, lat, lon, sog, vessel_group  (any vessel's fixes)
    -> gap_id, nb_mmsi, nb_group, side_start, side_end, min_dist_nm, closest_epoch, closest_lat, closest_lon
    Spatial join on a grid: each probe is expanded to the 5x5 neighbourhood of
    cells and the hours its window overlaps, then exact haversine distance.
    """
    probes = (gaps.select("gap_id", F.col("mmsi").alias("g_mmsi"), F.lit("start").alias("side"),
                          F.col("start_epoch").alias("t"), F.col("start_lat").alias("plat"),
                          F.col("start_lon").alias("plon"))
              .unionByName(gaps.select("gap_id", F.col("mmsi").alias("g_mmsi"), F.lit("end").alias("side"),
                                       F.col("end_epoch").alias("t"), F.col("end_lat").alias("plat"),
                                       F.col("end_lon").alias("plon"))))
    cl, cn = _cells("plat", "plon", cell_deg)
    offsets = F.array(*[F.lit(i) for i in range(-2, 3)])
    probes = (probes
              .withColumn("cell_lat", F.explode(F.transform(offsets, lambda o: o + cl)))
              .withColumn("cell_lon", F.explode(F.transform(offsets, lambda o: o + cn)))
              .withColumn("hour", F.explode(F.sequence(F.floor((F.col("t") - window_s) / 3600),
                                                       F.floor((F.col("t") + window_s) / 3600)))))
    pl, pn = _cells("lat", "lon", cell_deg)
    pts = (points.select("mmsi", "epoch", "lat", "lon", "sog", "vessel_group")
           .withColumn("cell_lat", pl).withColumn("cell_lon", pn)
           .withColumn("hour", F.floor(F.col("epoch") / 3600)))
    j = (probes.join(pts, ["cell_lat", "cell_lon", "hour"])
         .filter((F.col("mmsi") != F.col("g_mmsi")) & (F.abs(F.col("epoch") - F.col("t")) <= window_s))
         .withColumn("d", haversine_nm(F.col("plat"), F.col("plon"), F.col("lat"), F.col("lon")))
         .filter(F.col("d") <= radius_nm))
    return (j.groupBy("gap_id", F.col("mmsi").alias("nb_mmsi"))
            .agg(F.first("vessel_group").alias("nb_group"),
                 F.max((F.col("side") == "start").cast("int")).alias("side_start"),
                 F.max((F.col("side") == "end").cast("int")).alias("side_end"),
                 F.round(F.min("d"), 2).alias("min_dist_nm"),
                 F.min_by(F.struct("epoch", "lat", "lon"), "d").alias("c"))
            .select("gap_id", "nb_mmsi", "nb_group", "side_start", "side_end", "min_dist_nm",
                    F.col("c.epoch").alias("closest_epoch"), F.col("c.lat").alias("closest_lat"),
                    F.col("c.lon").alias("closest_lon")))


def reporting_during(gaps: DataFrame, neighbours: DataFrame, points: DataFrame, radius_nm: float,
                     pad_s: int) -> DataFrame:
    """For each (gap, neighbour): reports during the gap, reports within the
    radius of the gap start/end position during the gap, and the neighbour's
    report times in [start - pad, end + pad] (for the timeline)."""
    g = gaps.select("gap_id", "start_epoch", "end_epoch", "start_lat", "start_lon", "end_lat", "end_lon")
    p = points.select(F.col("mmsi").alias("nb_mmsi"), "epoch", "lat", "lon")
    j = (neighbours.select("gap_id", "nb_mmsi").join(g, "gap_id").join(p, "nb_mmsi")
         .filter((F.col("epoch") >= F.col("start_epoch") - pad_s) & (F.col("epoch") <= F.col("end_epoch") + pad_s)))
    inside = (F.col("epoch") > F.col("start_epoch")) & (F.col("epoch") < F.col("end_epoch"))
    near = ((haversine_nm(F.col("start_lat"), F.col("start_lon"), F.col("lat"), F.col("lon")) <= radius_nm)
            | (haversine_nm(F.col("end_lat"), F.col("end_lon"), F.col("lat"), F.col("lon")) <= radius_nm))
    return (j.groupBy("gap_id", "nb_mmsi")
            .agg(F.sum(inside.cast("int")).alias("reports_during"),
                 F.sum((inside & near).cast("int")).alias("reports_near_during"),
                 F.array_sort(F.collect_list("epoch")).alias("timeline")))


def summarise(gaps: DataFrame, neighbours: DataFrame, during: DataFrame, target_points: DataFrame,
              max_neighbours: int, pad_s: int) -> DataFrame:
    nb = (neighbours.join(during, ["gap_id", "nb_mmsi"], "left")
          .fillna(0, ["reports_during", "reports_near_during"]))
    # keep the closest neighbours for display; counts use all of them
    w = Window.partitionBy("gap_id").orderBy("min_dist_nm", "nb_mmsi")
    nb = nb.withColumn("_rk", F.row_number().over(w))
    counts = nb.groupBy("gap_id").agg(
        F.count(F.lit(1)).alias("neighbours_total"),
        F.sum("side_start").alias("neighbours_at_start"),
        F.sum("side_end").alias("neighbours_at_end"),
        F.sum((F.col("reports_during") > 0).cast("int")).alias("neighbours_reporting_during"),
        F.sum((F.col("reports_near_during") > 0).cast("int")).alias("neighbours_near_during"),
        F.round(F.min("min_dist_nm"), 2).alias("closest_neighbour_nm"))
    shown = (nb.filter(F.col("_rk") <= max_neighbours)
             .groupBy("gap_id")
             .agg(F.to_json(F.array_sort(F.collect_list(F.struct(
                 "min_dist_nm", "nb_mmsi", "nb_group", "side_start", "side_end", "closest_epoch",
                 "closest_lat", "closest_lon", "reports_during", "reports_near_during",
                 F.coalesce(F.col("timeline"), F.array().cast("array<long>")).alias("timeline"))))).alias("neighbours_json")))
    tp = target_points.select(F.col("mmsi"), F.col("epoch").alias("te"))
    target = (gaps.select("gap_id", "mmsi", "start_epoch", "end_epoch").join(tp, "mmsi")
              .filter((F.col("te") >= F.col("start_epoch") - pad_s) & (F.col("te") <= F.col("end_epoch") + pad_s))
              .groupBy("gap_id").agg(F.to_json(F.array_sort(F.collect_list("te"))).alias("target_timeline_json")))
    out = (gaps.join(counts, "gap_id", "left").join(shown, "gap_id", "left").join(target, "gap_id", "left")
           .fillna(0, ["neighbours_total", "neighbours_at_start", "neighbours_at_end",
                       "neighbours_reporting_during", "neighbours_near_during"])
           .withColumn("visibility", F.when(F.col("neighbours_near_during") > 0, F.lit("coverage_observed"))
                       .when(F.col("neighbours_total") > 0, F.lit("coverage_unconfirmed"))
                       .otherwise(F.lit("isolated"))))
    label = F.create_map(*[x for k, v in LABELS.items() for x in (F.lit(k), F.lit(v))])
    return out.withColumn("visibility_text", label[F.col("visibility")])


def run(spark=None):
    cfg = load_config()["visibility"]
    spark = spark or get_spark("11_ais_visibility")
    with Timer() as t:
        gaps = (read_csv(spark, "gap_risk")
                .filter(F.col("risk_score") >= cfg["min_risk_score"])
                .select("gap_id", "mmsi", "vessel_group", "risk_score", "risk_level", "gap_h",
                        "start_epoch", "end_epoch", "start_lat", "start_lon", "end_lat", "end_lon")
                .cache())
        points = read_csv(spark, "vessel_track_points").cache()     # 15-min last fix per vessel
        nb = find_neighbours(gaps, points, cfg["radius_nm"], cfg["window_minutes"] * 60, cfg["cell_deg"]).cache()
        during = reporting_during(gaps, nb, points, cfg["radius_nm"], cfg["timeline_pad_minutes"] * 60)
        out = summarise(gaps, nb, during, points, cfg["max_neighbours"], cfg["timeline_pad_minutes"] * 60)
        write_csv(out, "ais_visibility", coalesce=2)
        s = (read_csv(spark, "ais_visibility").groupBy("visibility").agg(
            F.count(F.lit(1)).alias("n"), F.sum((F.col("risk_level") == "High Priority").cast("int")).alias("hp"))
             .collect())
        n_gaps = sum(r["n"] for r in s)
        gaps.unpersist(); points.unpersist(); nb.unpersist()
    record_metrics(spark, "ais_visibility", {
        "elevated_gaps": n_gaps,
        "by_visibility": {r["visibility"]: {"gaps": r["n"], "high_priority": r["hp"]} for r in s},
        "radius_nm": cfg["radius_nm"], "window_minutes": cfg["window_minutes"],
        "elapsed_seconds": t.seconds,
    })


if __name__ == "__main__":
    run()
