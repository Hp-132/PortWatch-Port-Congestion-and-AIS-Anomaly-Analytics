# Pipeline

The MARITIME-X batch pipeline runs **12 Spark stages in a fixed order inside one
SparkSession**. The runner lives at [`src/pipeline.py`](../src/pipeline.py) — it is *not*
moved into this folder, because every stage is imported as a module of the `src` package.
This folder documents the stage order and how to run it.

Each stage reads its inputs from the data lake (`output/lake/` locally,
`hdfs:///maritime/` on the cluster) and writes its outputs back, so any stage can also be
submitted on its own. Every stage appends a row set to
`output/pipeline_metrics/<stage>.csv` (snapshot of our last full run is committed).

## The 12 stages, in order

| # | Stage name | Module | What it does |
|---|---|---|---|
| 1 | `clean_ais` | `src.preprocessing.clean_ais` | Reads the 47.9 M raw rows, validates MMSI (9 digits, MID 200–799), drops invalid timestamps and coordinates, nullifies impossible SOG/COG/heading, builds the vessel registry. Output: 47,912,404 clean rows, 24,529 vessels. |
| 2 | `trajectories` | `src.preprocessing.trajectories` | Orders each vessel's fixes by time, computes inter-fix distance/time/implied speed, and labels each fix MOVING / ANCHOR / BERTH from SOG thresholds. |
| 3 | `port_visits` | `src.analytics.turnaround.port_visits` | Joins fixes against the 38 port geofences in `config/ports.csv`, groups them into visits, and classifies each as PASS_THROUGH, ANCHORED_ONLY or BERTHED. |
| 4 | `port_congestion` | `src.analytics.congestion.port_congestion` | Hourly and daily queue length, waiting time, dwell and berth utilisation per port, combined into the project-defined Port Congestion Index (PCI). |
| 5 | `turnaround` | `src.analytics.turnaround.turnaround` | Per-port turnaround statistics (waiting, berth, total time) for Cargo and Tanker visits, with censored visits flagged. |
| 6 | `gaps` | `src.analytics.dark_vessel.gaps` | Detects AIS reporting gaps per vessel (>60 min Class A, >120 min Class B). Output: 48,772 gaps across 11,328 vessels. |
| 7 | `kmeans` | `src.ml.gap_kmeans` | Builds gap feature vectors and fits K-Means (k selected from 4–8, seed 42) to produce an unsupervised ML anomaly score per gap. |
| 8 | `risk_scoring` | `src.analytics.dark_vessel.risk_scoring` | Blends the rule score and the ML score (0.6 / 0.4) into a 0–100 risk score and the levels Normal / Watch / Suspicious / High Priority. |
| 9 | `identity` | `src.analytics.dark_vessel.identity` | Flags potential MMSI identity inconsistencies from implausible implied speeds between consecutive fixes (never treated as proof of spoofing). |
| 10 | `serving_views` | `src.database.build_serving_views` | Reduces the analytics tables to the small, map-ready serving views the dashboard reads (vessel summary, positions, track points, daily trends, hourly activity). |
| 11 | `ais_visibility` | `src.analytics.dark_vessel.visibility` | For Suspicious / High Priority gaps, finds vessels that were reporting nearby (10 nm, ±30 min) around the gap start and end. |
| 12 | `load_mongodb` | `src.database.load_mongodb` | Loads the serving views into MongoDB (`maritime_x`) and creates the indexes the API queries. |

## Running it

### Local (single machine, `local[12]`)

```powershell
. .\scripts\env_local.ps1
.\scripts\run_pipeline_local.ps1
```

### On the 3-node YARN cluster

```powershell
.\scripts\start_all.ps1            # namenode, 2 datanodes, resourcemanager, 2 nodemanagers
.\scripts\cluster.ps1 -Action status
python .\scripts\cluster_smoke.py  # confirm Spark-on-YARN works before the real run
.\scripts\run_pipeline_cluster.ps1
.\scripts\stop_all.ps1
```

### Running stages selectively

```powershell
python -m src.pipeline                        # all 12 stages
python -m src.pipeline --from port_visits     # resume from stage 3 onward
python -m src.pipeline --only gaps,kmeans     # just those stages
```

`--from` is the normal way to resume after a failure: the completed stages' outputs are
already in the lake, so there is no need to re-read the raw 4.9 GB file.

## Tuning

Every threshold (cleaning limits, PCI weights, gap minutes, risk weights, K-Means
candidates, MongoDB URI) lives in [`config/pipeline.yaml`](../config/pipeline.yaml), not in
the code. Spark sizing for the cluster is in the same file under `spark.cluster`.
Environment overrides: `MX_RAW_INPUT`, `MX_DATA_ROOT`, `MX_SPARK_MASTER`, `MX_MONGO_URI`,
`MX_MONGO_DB`.

See [`../docs/methodology.md`](../docs/methodology.md) for how each metric is defined.
