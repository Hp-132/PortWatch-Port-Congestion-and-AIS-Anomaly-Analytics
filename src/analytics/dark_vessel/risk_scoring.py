"""Stage 8 - AIS gap risk scoring (rules + MLlib), 0-100.

processed/ais_gaps + ml/features/gap_ml_scores -> analytics/dark_vessel/gap_risk

risk_score = rule_weight * rule_score + ml_weight * ml_score     (config)

rule_score (0-100) is a weighted sum of explainable components in [0, 1]:

  duration   0.25  clip(ln(gap_h) / ln(48), 0, 1)        longer gaps rank higher
  underway   0.15  clip(sog_before / 2 kn, 0, 1)         vessel was moving when it went silent
  coverage   0.15  clip(ln(1+density)/ln(1+P95 density)) other vessels WERE received nearby,
                                                         so a coverage hole is less likely
  speed      0.15  clip(speed_mismatch / max(avg sog,5)) implied speed does not match reports
                   (1.0 if implied speed > 35 kn)
  course     0.10  course_change / 180
  offshore   0.05  clip(dist_to_port / 50 nm, 0, 1)
  night      0.05  started at local night
  repeat     0.10  clip(vessel gaps per day / 5, 0, 1)

Gaps where the vessel was stationary inside a port on both sides are scaled
by 0.3 (routine low-rate reporting at berth/anchor).

Risk levels: 0-30 Normal | 31-60 Watch | 61-80 Suspicious | 81-100 High Priority.
These are review-priority labels, not findings of wrongdoing.
"""
from __future__ import annotations

import math

from pyspark.sql import functions as F

from src.common.config import load_config
from src.common.metrics import Timer, record_metrics
from src.common.spark import get_spark, read_csv, write_csv

RULE_WEIGHTS = {"duration": 0.25, "underway": 0.15, "coverage": 0.15, "speed": 0.15,
                "course": 0.10, "offshore": 0.05, "night": 0.05, "repeat": 0.10}
FACTOR_TEXT = {
    "duration": "long AIS gap", "underway": "underway when transmissions stopped",
    "coverage": "other vessels received nearby", "speed": "implied speed inconsistent with reported speed",
    "course": "large course change across gap", "offshore": "away from port",
    "night": "started at night (local time)", "repeat": "vessel has frequent gaps",
}


def _clip01(c):
    return F.greatest(F.lit(0.0), F.least(F.lit(1.0), c))


def rule_components(df, density_p95: float):
    avg_sog = F.coalesce(F.col("avg_reported_sog"), F.lit(0.0))
    return (df
            .withColumn("c_duration", _clip01(F.log(F.col("gap_h")) / math.log(48)))
            .withColumn("c_underway", _clip01(F.coalesce("sog_before", F.lit(0.0)) / 2.0))
            .withColumn("c_coverage", _clip01(F.log1p("density_start") / math.log1p(max(density_p95, 1.0))))
            .withColumn("c_speed", F.when(F.col("dist_nm") < 1.0, F.lit(0.0))
                        .when(F.coalesce("implied_kn", F.lit(0.0)) > 35, F.lit(1.0))
                        .otherwise(_clip01(F.col("speed_mismatch_kn") / F.greatest(avg_sog, F.lit(5.0)))))
            .withColumn("c_course", _clip01(F.coalesce("course_change_deg", F.lit(0.0)) / 180.0))
            .withColumn("c_offshore", _clip01(F.coalesce("start_dist_to_port_nm", F.lit(50.0)) / 50.0))
            .withColumn("c_night", F.col("night").cast("double"))
            .withColumn("c_repeat", _clip01(F.col("vessel_gaps_per_day") / 5.0)))


def risk_level(score_col, levels: dict):
    expr = None
    for name, upper in sorted(levels.items(), key=lambda kv: kv[1]):
        cond = score_col <= upper
        expr = F.when(cond, F.lit(name)) if expr is None else expr.when(cond, F.lit(name))
    return expr.otherwise(F.lit(list(levels)[-1]))


def score(df, density_p95: float, rule_w: float, ml_w: float, levels: dict):
    df = rule_components(df, density_p95)
    rule = sum(F.lit(w) * F.col(f"c_{k}") for k, w in RULE_WEIGHTS.items())
    damp = F.when(F.col("stationary_in_port") == 1, F.lit(0.3)).otherwise(F.lit(1.0))
    df = (df.withColumn("rule_score", F.round(100 * rule * damp, 2))
          .withColumn("ml_score", F.round(F.coalesce("ml_score", F.lit(0.0)) * damp, 2))
          .withColumn("risk_score", F.round(rule_w * F.col("rule_score") + ml_w * F.col("ml_score")).cast("int")))
    df = df.withColumn("risk_level", risk_level(F.col("risk_score"), levels))

    # Explanation: components contributing >= 5 points to the rule score, largest first.
    contrib = F.array(*[F.struct((F.lit(100 * w) * F.col(f"c_{k}") * damp).alias("v"), F.lit(FACTOR_TEXT[k]).alias("t"))
                        for k, w in RULE_WEIGHTS.items()])
    df = df.withColumn("_c", F.reverse(F.array_sort(contrib)))
    df = df.withColumn("explanation", F.concat_ws("; ", F.transform(
        F.filter("_c", lambda s: s["v"] >= 5), lambda s: s["t"])))
    df = df.withColumn("explanation", F.when(F.col("stationary_in_port") == 1,
                                             F.concat_ws("; ", F.lit("stationary in port on both sides (down-weighted)"),
                                                         F.col("explanation")))
                       .otherwise(F.col("explanation"))).drop("_c")
    return df


def run(spark=None):
    cfg = load_config()["dark_vessel"]
    spark = spark or get_spark("08_risk_scoring")
    with Timer() as t:
        gaps = read_csv(spark, "ais_gaps")
        ml = read_csv(spark, "gap_ml_scores")
        density_p95 = gaps.approxQuantile("density_start", [0.95], 0.001)[0]
        scored = score(gaps.join(ml, "gap_id", "left"), density_p95,
                       cfg["rule_weight"], cfg["ml_weight"], cfg["risk_levels"])
        scored = scored.withColumn("label", F.lit("AIS gap - review priority, not evidence of wrongdoing")).cache()
        write_csv(scored, "gap_risk", coalesce=8)
        levels = {r["risk_level"]: r["count"] for r in scored.groupBy("risk_level").count().collect()}
        corr = scored.stat.corr("rule_score", "ml_score")
        scored.unpersist()

    record_metrics(spark, "risk_scoring", {
        "gaps_by_risk_level": levels, "rule_ml_correlation": round(corr, 4),
        "density_p95": density_p95, "rule_weight": cfg["rule_weight"], "ml_weight": cfg["ml_weight"],
        "elapsed_seconds": t.seconds,
    })


if __name__ == "__main__":
    run()
