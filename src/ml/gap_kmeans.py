"""Stage 7 - Spark MLlib: unsupervised clustering of AIS gaps.

ml/features/gap_features -> ml/features/gap_ml_scores
                          -> ml/models/gap_kmeans (model parameters as JSON)
                          -> analytics/dark_vessel/cluster_profiles

Pipeline: VectorAssembler -> StandardScaler(mean, std) -> KMeans
  * k is selected from config candidates by silhouette score (ClusteringEvaluator)
    on a sample of the scaled feature vectors.
  * ML anomaly signal per gap = Euclidean distance to its assigned centroid in
    standardised space, plus a cluster-rarity term:
        ml_score = 100 * (0.75 * clip(distance / P99(distance), 0, 1)
                          + 0.25 * (1 - cluster_size / largest_cluster_size))
  * Each cluster is described by the features whose centroid z-scores deviate
    most from the population mean (explainable clusters).

There are no ground-truth labels, so this is unsupervised ranking, not a
validated classifier.

Model persistence: MLlib's native ``model.save()`` stores data as Parquet,
which this project does not use. The fitted parameters (scaler mean/std,
centroids, selected k, silhouette scores) are therefore exported as JSON,
which is sufficient to reproduce the scoring.
"""
from __future__ import annotations

import json
import math

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from src.analytics.dark_vessel.gaps import FEATURE_COLS
from src.common.config import load_config
from src.common.metrics import Timer, record_metrics
from src.common.paths import lake_path
from src.common.spark import get_spark, read_csv, write_csv

FEATURE_LABELS = {
    "f_log_gap_h": "gap duration", "f_log_dist": "distance across gap",
    "f_implied_kn": "implied speed", "f_sog_before": "speed before gap",
    "f_sog_after": "speed after gap", "f_speed_mismatch": "implied vs reported speed mismatch",
    "f_course_change": "course change", "f_log_port_dist": "distance from port",
    "f_log_density": "local traffic density", "f_night": "night-time start",
    "f_log_gap_rate": "vessel gap frequency",
}
SILHOUETTE_SAMPLE_ROWS = 200_000


def build_pipeline(k: int, seed: int) -> Pipeline:
    return Pipeline(stages=[
        VectorAssembler(inputCols=FEATURE_COLS, outputCol="features", handleInvalid="skip"),
        StandardScaler(inputCol="features", outputCol="scaled", withMean=True, withStd=True),
        KMeans(featuresCol="scaled", predictionCol="cluster", k=k, seed=seed, maxIter=40),
    ])


def centroid_distance(df, centers: list[list[float]]):
    """Distance from the 'scaled' vector to its cluster centre (native Spark exprs)."""
    centers_lit = F.array(*[F.array(*[F.lit(float(v)) for v in c]) for c in centers])
    return (df.withColumn("_v", vector_to_array("scaled"))
            .withColumn("_c", F.element_at(centers_lit, F.col("cluster") + 1))
            .withColumn("centroid_distance", F.round(F.sqrt(F.aggregate(
                F.zip_with("_v", "_c", lambda a, b: (a - b) * (a - b)), F.lit(0.0), lambda acc, x: acc + x)), 4))
            .drop("_v", "_c"))


def describe_cluster(z: dict[str, float]) -> str:
    top = sorted(z.items(), key=lambda kv: -abs(kv[1]))[:3]
    parts = [f"{'high' if v > 0 else 'low'} {FEATURE_LABELS[f]}" for f, v in top if abs(v) >= 0.5]
    return ", ".join(parts) if parts else "typical gap profile"


def run(spark=None):
    cfg = load_config()["dark_vessel"]
    spark = spark or get_spark("07_gap_kmeans")
    with Timer() as t:
        feats = read_csv(spark, "gap_features").na.drop(subset=FEATURE_COLS)
        if cfg["kmeans_sample_fraction"] < 1.0:
            feats = feats.sample(fraction=cfg["kmeans_sample_fraction"], seed=cfg["kmeans_seed"])
        feats = feats.cache()
        n = feats.count()

        # ---- choose k by silhouette on a sample --------------------------
        prep = Pipeline(stages=build_pipeline(2, 1).getStages()[:2]).fit(feats)
        sample = prep.transform(feats.sample(fraction=min(1.0, SILHOUETTE_SAMPLE_ROWS / max(n, 1)),
                                             seed=cfg["kmeans_seed"])).cache()
        evaluator = ClusteringEvaluator(featuresCol="scaled", predictionCol="cluster",
                                        metricName="silhouette")
        silhouettes = {}
        for k in cfg["kmeans_k_candidates"]:
            m = KMeans(featuresCol="scaled", predictionCol="cluster", k=k, seed=cfg["kmeans_seed"],
                       maxIter=40).fit(sample)
            silhouettes[k] = round(evaluator.evaluate(m.transform(sample)), 4)
            print(f"[kmeans] k={k} silhouette={silhouettes[k]}")
        best_k = max(silhouettes, key=silhouettes.get)
        sample.unpersist()

        # ---- fit final pipeline on all gaps -------------------------------
        model = build_pipeline(best_k, cfg["kmeans_seed"]).fit(feats)
        scaler_model, km_model = model.stages[1], model.stages[2]
        centers = [list(map(float, c)) for c in km_model.clusterCenters()]
        scored = centroid_distance(model.transform(feats), centers)

        sizes = {r["cluster"]: r["count"] for r in scored.groupBy("cluster").count().collect()}
        largest = max(sizes.values())
        p99 = scored.approxQuantile("centroid_distance", [0.99], 0.001)[0] or 1.0
        rarity = F.create_map(*[x for c, s in sizes.items() for x in (F.lit(c), F.lit(1 - s / largest))])
        scored = (scored
                  .withColumn("cluster_rarity", F.round(rarity[F.col("cluster")], 4))
                  .withColumn("distance_norm", F.round(F.least(F.lit(1.0), F.col("centroid_distance") / p99), 4))
                  .withColumn("ml_score", F.round(100 * (0.75 * F.col("distance_norm")
                                                         + 0.25 * F.col("cluster_rarity")), 2)))
        write_csv(scored.select("gap_id", "cluster", "centroid_distance", "distance_norm",
                                "cluster_rarity", "ml_score"), "gap_ml_scores")

        # ---- cluster interpretation ---------------------------------------
        means = scored.groupBy("cluster").agg(
            F.count(F.lit(1)).alias("size"),
            *[F.round(F.avg(c), 4).alias(c) for c in FEATURE_COLS],
            F.round(F.avg("centroid_distance"), 4).alias("avg_centroid_distance"),
            F.round(F.avg("ml_score"), 2).alias("avg_ml_score")).collect()
        profiles = []
        for r in means:
            c = r["cluster"]
            z = dict(zip(FEATURE_COLS, centers[c]))   # centroid in z-score space
            profiles.append({
                "cluster": c, "size": r["size"], "share": round(r["size"] / n, 4),
                "description": describe_cluster(z),
                "avg_centroid_distance": r["avg_centroid_distance"], "avg_ml_score": r["avg_ml_score"],
                # Interpretable raw-feature means (back-transformed where logged)
                "mean_gap_h": round(float(math.exp(r["f_log_gap_h"])), 2),
                "mean_dist_nm": round(float(math.expm1(r["f_log_dist"])), 2),
                "mean_implied_kn": r["f_implied_kn"], "mean_sog_before": r["f_sog_before"],
                "mean_speed_mismatch": r["f_speed_mismatch"], "mean_course_change": r["f_course_change"],
                "mean_dist_to_port_nm": round(float(math.expm1(r["f_log_port_dist"])), 2),
                "mean_density": round(float(math.expm1(r["f_log_density"])), 2),
                "night_share": r["f_night"],
                "centroid_z": json.dumps({k: round(v, 3) for k, v in z.items()}),
            })
        prof_df = spark.createDataFrame(profiles)
        write_csv(prof_df, "cluster_profiles", coalesce=1)

        model_json = {
            "algorithm": "KMeans (Spark MLlib)", "k": best_k, "silhouette_by_k": silhouettes,
            "features": FEATURE_COLS, "scaler_mean": list(map(float, scaler_model.mean)),
            "scaler_std": list(map(float, scaler_model.std)), "centers_scaled": centers,
            "training_rows": n, "distance_p99": p99, "cluster_sizes": sizes,
            "training_cost": float(km_model.summary.trainingCost),
        }
        spark.createDataFrame([(json.dumps(model_json),)], "json string").coalesce(1) \
            .write.mode("overwrite").text(lake_path("kmeans_model"))
        feats.unpersist()

    record_metrics(spark, "gap_kmeans", {
        "training_rows": n, "selected_k": best_k, "silhouette_by_k": silhouettes,
        "cluster_sizes": sizes, "elapsed_seconds": t.seconds,
    })


if __name__ == "__main__":
    run()
