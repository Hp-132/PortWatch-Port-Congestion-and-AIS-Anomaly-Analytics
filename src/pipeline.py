"""MARITIME-X pipeline runner.

Runs the Spark stages in order inside ONE SparkSession (local or cluster):

    python -m src.pipeline                      # all stages
    python -m src.pipeline --from port_visits   # resume from a stage
    python -m src.pipeline --only gaps,kmeans   # selected stages

Each stage reads its inputs from the lake (local folder or HDFS) and writes
its outputs back, so stages can also be submitted individually with
spark-submit on the cluster.
"""
from __future__ import annotations

import argparse
import importlib
import time

from src.common.spark import get_spark

STAGES = [
    ("clean_ais", "src.preprocessing.clean_ais"),
    ("trajectories", "src.preprocessing.trajectories"),
    ("port_visits", "src.analytics.turnaround.port_visits"),
    ("port_congestion", "src.analytics.congestion.port_congestion"),
    ("turnaround", "src.analytics.turnaround.turnaround"),
    ("gaps", "src.analytics.dark_vessel.gaps"),
    ("kmeans", "src.ml.gap_kmeans"),
    ("risk_scoring", "src.analytics.dark_vessel.risk_scoring"),
    ("identity", "src.analytics.dark_vessel.identity"),
    ("serving_views", "src.database.build_serving_views"),
    ("ais_visibility", "src.analytics.dark_vessel.visibility"),
    ("load_mongodb", "src.database.load_mongodb"),
]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="start", default=None, help="first stage to run")
    ap.add_argument("--only", default=None, help="comma separated stage names")
    args = ap.parse_args()

    names = [n for n, _ in STAGES]
    selected = names
    if args.only:
        wanted = [x.strip() for x in args.only.replace(" ", ",").split(",") if x.strip()]
        unknown = set(wanted) - set(names)
        if unknown:
            ap.error(f"unknown stage(s) {sorted(unknown)}; choose from {names}")
        selected = [n for n in names if n in wanted]
    elif args.start:
        selected = names[names.index(args.start):]

    spark = get_spark("pipeline")
    print(f"Spark {spark.version} master={spark.sparkContext.master} "
          f"defaultParallelism={spark.sparkContext.defaultParallelism}")
    for name, module in STAGES:
        if name not in selected:
            continue
        t0 = time.time()
        print(f"\n===== stage {name} =====")
        spark.sparkContext.setJobGroup(name, f"MARITIME-X stage {name}")
        importlib.import_module(module).run(spark)
        print(f"===== {name} done in {time.time() - t0:.0f}s =====")
    spark.stop()


if __name__ == "__main__":
    main()
