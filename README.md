# MARITIME-X

Big-data analytics on 47.9 million AIS vessel position reports: a Hadoop/YARN + Spark
batch pipeline that cleans the raw feed, reconstructs vessel trajectories, measures port
congestion and turnaround, detects "dark vessel" AIS gaps with a rule + K-Means risk
model, serves the results from MongoDB and visualises them in a 5-page web dashboard.

| | |
|---|---|
| Dataset | 47,958,624 AIS position reports, 17 columns, 24,688 MMSIs, 2025-01-01 → 2025-01-07 UTC, 4.9 GB CSV |
| Cluster | 3 nodes (1 master + 2 workers) — HDFS, YARN, Spark on YARN, on one 16 GB Windows host |
| Pipeline | 12 Spark stages in one SparkSession, resumable per stage |
| Serving | MongoDB (`maritime_x`) + FastAPI REST API |
| Dashboard | Static JS front end, 5 pages, MapLibre GL map |

The raw CSV is **not** in this repository — see [dataset/README.md](dataset/README.md).

## Repository layout

```
README.md                 this file
requirements.txt          python dependencies
dataset/                  dataset description + 1,000-row sample (raw CSV not committed)
src/
  pipeline.py             the 12-stage runner
  common/                 config, paths, Spark session, geo helpers, schemas, metrics
  ingestion/              dataset profiling, port hotspot discovery
  preprocessing/          clean_ais.py, trajectories.py
  analytics/
    ports.py              port geofence joins
    turnaround/           port_visits.py, turnaround.py
    congestion/           port_congestion.py
    dark_vessel/          gaps.py, risk_scoring.py, identity.py, visibility.py
  ml/                     gap_kmeans.py
  database/               build_serving_views.py, load_mongodb.py
pipeline/                 stage-by-stage documentation and run instructions
config/                   pipeline.yaml (all thresholds), ports.csv (38 port geofences)
cluster/conf/             Hadoop/YARN configs for master, worker1, worker2
scripts/                  cluster control, environment setup, pipeline and dashboard runners
dashboard/
  backend/                FastAPI app, MongoDB access, insight text
  frontend/               index.html, js/ (app, api, map, theme, views/)
output/
  pipeline_metrics/       per-stage metrics from our last full run
  reports/                port hotspot and port reference checks
  logs/                   pipeline and dashboard run logs
  lake/                   the data lake (~12 GB, not committed)
tests/                    pytest suite
notebooks/                01_cluster_and_pipeline, 02_results_analysis
docs/                     dataset, methodology, CONTRIBUTIONS
screenshots/              cluster, YARN, Spark, MongoDB and dashboard evidence
```

## Quick start

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Place `marine_ais_dataset.csv` in the project root ([instructions](dataset/README.md)), then:

```powershell
. .\scripts\env_local.ps1          # local Spark (local[12])
.\scripts\run_pipeline_local.ps1   # all 12 stages
.\scripts\run_dashboard.ps1        # http://localhost:8000
```

On the cluster:

```powershell
.\scripts\start_all.ps1
.\scripts\cluster.ps1 -Action status
python .\scripts\cluster_smoke.py
.\scripts\run_pipeline_cluster.ps1
```

Stage order, resuming and tuning: [pipeline/README.md](pipeline/README.md).

## Pipeline stages

`clean_ais` → `trajectories` → `port_visits` → `port_congestion` → `turnaround` → `gaps`
→ `kmeans` → `risk_scoring` → `identity` → `serving_views` → `ais_visibility` →
`load_mongodb`

## Dashboard pages

| Page | Contents |
|---|---|
| Overview | Fleet-wide KPIs, activity timeline, live vessel position map |
| Ports | Port Congestion Index, queue length, waiting and dwell time, turnaround per port |
| Vessels | Vessel search, vessel profile, track playback on the map |
| Trends | Daily and hourly traffic trends by vessel group |
| Anomalies | AIS gaps, risk levels, K-Means clusters, identity events, visibility around gaps |

Screenshots of all five pages are in [screenshots/](screenshots/).

## Results

From the last full run (`output/pipeline_metrics/`):

| Metric | Value |
|---|---|
| Raw rows read | 47,958,624 |
| Clean rows | 47,912,404 (46,220 removed, 36,765 of them invalid MMSI) |
| Vessels in registry | 24,529 |
| Port-zone fixes | 2,094,457 (1,318,119 at berth, 503,536 at anchor, 272,802 moving) |
| Port visits | 5,259 (3,138 pass-through, 1,693 berthed, 428 anchored only) |
| AIS gaps detected | 48,772 across 11,328 vessels (median 4.1 h, p99 94.6 h) |
| Gaps by risk level | 20,515 Normal, 25,568 Watch, 2,625 Suspicious, 64 High Priority |
| Rule / ML score correlation | 0.777 |

How each number is defined: [docs/methodology.md](docs/methodology.md).

## Limitations

- **One week of data.** Seasonality, maintenance windows and holiday effects cannot be
  separated from genuine congestion.
- **AIS coverage is not uniform.** A reporting gap can mean a switched-off transceiver, or
  simply a vessel out of terrestrial receiver range. The risk score down-weights gaps that
  are plausibly coverage-driven, but it cannot prove intent — nothing here is evidence of
  illegal activity.
- **The Port Congestion Index is project-defined**, not an industry standard. Its four
  weights (queue 0.35, waiting 0.30, dwell 0.15, utilisation 0.20) are our engineering
  choice and are documented in `config/pipeline.yaml`.
- **Berth capacity is a proxy** (95th percentile of observed hourly at-berth counts), not
  real terminal capacity.
- **Identity flags are "potential inconsistencies"** derived from implausible implied
  speed, not confirmed MMSI spoofing.
- **Port geofences are curated** (38 ports, circular zones). Vessels calling at ports
  outside that list are not counted as visits.
- **Censored visits.** Visits that start or end within 2 hours of the dataset window edges
  have unobservable true arrival/departure times and are flagged, not imputed.
- **Cluster scale.** The 3 "nodes" are processes on one 16 GB host, so wall-clock timings
  demonstrate distribution, not production throughput.

## Documentation

| Document | Contents |
|---|---|
| [docs/dataset.md](docs/dataset.md) | Full data profile, null rates, value ranges |
| [docs/methodology.md](docs/methodology.md) | Definition of every metric and threshold |
| [docs/CONTRIBUTIONS.md](docs/CONTRIBUTIONS.md) | Who built what |
| [pipeline/README.md](pipeline/README.md) | The 12 stages and how to run them |
| [dataset/README.md](dataset/README.md) | Obtaining and placing the raw CSV |

## Team

| Member | Role |
|---|---|
| Member 1 | Cluster & data engineering — Hadoop/YARN, ingestion, cleaning, trajectories |
| Member 2 | Port analytics & serving layer — visits, congestion, turnaround, MongoDB, API |
| Member 3 | Anomaly detection, ML & dashboard — AIS gaps, risk model, front end |

Full breakdown in [docs/CONTRIBUTIONS.md](docs/CONTRIBUTIONS.md).

## Tests

```powershell
.\scripts\run_tests.ps1
```
