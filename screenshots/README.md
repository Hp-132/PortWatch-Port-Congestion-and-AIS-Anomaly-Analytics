# Screenshots

Evidence that the cluster, the pipeline and the dashboard actually ran. Captured on the
3-node Windows Hadoop/YARN cluster (1 master + 2 workers on one 16 GB host).

## Cluster and infrastructure

| File | What it shows | How to capture |
|---|---|---|
| `cluster_status.png` | All 6 daemons up (namenode, 2 datanodes, resourcemanager, 2 nodemanagers) | `.\scripts\cluster.ps1 -Action status` |
| `hdfs_dfsadmin_report.png` | HDFS capacity, live datanodes, per-node usage | `hdfs dfsadmin -report` |
| `hdfs_fsck_blocks.png` | Block distribution and replication for the raw AIS file | `hdfs fsck /maritime/raw/ais -files -blocks -locations` |
| `yarn_nodes.png` | YARN ResourceManager UI, both NodeManagers RUNNING | http://localhost:8088/cluster/nodes |
| `yarn_application.png` | The pipeline application in YARN with its container allocation | http://localhost:8088/cluster/apps |
| `spark_executors.png` | Spark UI Executors tab, 2 executors x 3 cores on the two workers | http://localhost:4040/executors |
| `cluster_smoke_output.png` | `cluster_smoke.py` passing on YARN | `python .\scripts\cluster_smoke.py` |
| `mongodb_counts.png` | Document counts per serving collection in `maritime_x` | `mongosh maritime_x --eval "db.getCollectionNames().forEach(c => print(c, db[c].countDocuments()))"` |

## Dashboard (5 pages)

| File | Page | Status |
|---|---|---|
| `dashboard_overview.png` | Overview — fleet KPIs, activity timeline, live position map | captured |
| `dashboard_ports.png` | Ports — congestion index, queue/dwell, turnaround per port | captured |
| `dashboard_vessels.png` | Vessels — vessel search, profile and track playback | captured |
| `dashboard_trends.png` | Trends — daily and hourly traffic by vessel group | captured |
| `dashboard_anomalies.png` | Anomalies — AIS gaps, risk levels, identity events, visibility | captured |

Start the dashboard with `.\scripts\run_dashboard.ps1` and open http://localhost:8000.

## Conventions

- PNG, full browser window or full console window, no cropping of titles or counts.
- Readable at 100 % zoom; nothing redacted except machine names if required.
- Keep the file names above exactly as listed — `docs/CONTRIBUTIONS.md` and the viva
  slides reference them.
