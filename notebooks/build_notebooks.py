"""Generate the MARITIME-X notebooks (run once; then execute with nbconvert).

    python notebooks/build_notebooks.py
    jupyter nbconvert --to notebook --execute --inplace notebooks/01_cluster_and_pipeline.ipynb
    jupyter nbconvert --to notebook --execute --inplace notebooks/02_results_analysis.ipynb

The notebooks never load the 48M raw rows: they query the cluster REST APIs
and the compact analytical results in MongoDB.
"""
from pathlib import Path

import nbformat as nbf

HERE = Path(__file__).parent


def nb(cells):
    n = nbf.v4.new_notebook()
    n.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
    n.cells = [nbf.v4.new_markdown_cell(c[1]) if c[0] == "md" else nbf.v4.new_code_cell(c[1]) for c in cells]
    return n


cluster = nb([
    ("md", "# MARITIME-X · 01 Cluster and pipeline verification\n\n"
           "Checks the 3-node Hadoop / YARN / Spark cluster through its REST APIs and shows the metrics each Spark "
           "stage recorded on the real AIS dataset. Start the cluster first: `.\\scripts\\cluster.ps1 start`."),
    ("code", "import json, urllib.request\nimport pandas as pd\nfrom pymongo import MongoClient\n\n"
             "def get(url):\n    with urllib.request.urlopen(url, timeout=15) as r:\n        return json.load(r)\n\n"
             "db = MongoClient('mongodb://localhost:27017')['maritime_x']"),
    ("md", "## HDFS: NameNode, DataNodes, blocks and replication"),
    ("code", "q = 'http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name='\n"
             "st = get(q + 'FSNamesystemState')['beans'][0]\nfs = get(q + 'FSNamesystem')['beans'][0]\n"
             "info = get(q + 'NameNodeInfo')['beans'][0]\n"
             "print('state', st['FSState'], '| live DataNodes', st['NumLiveDataNodes'], '| dead', st['NumDeadDataNodes'])\n"
             "print('files', st['FilesTotal'], '| blocks', st['BlocksTotal'], '| under-replicated', fs['UnderReplicatedBlocks'], '| missing', fs['MissingBlocks'])\n"
             "pd.DataFrame([{'datanode': v['xferaddr'], 'used_GB': round(v['used'] / 1e9, 2), 'blocks': v['numBlocks'], 'state': v['adminState']}\n"
             "              for v in json.loads(info['LiveNodes']).values()])"),
    ("code", "raw = get('http://localhost:9870/webhdfs/v1/maritime/raw/ais/marine_ais_dataset.csv?op=GETFILESTATUS')['FileStatus']\n"
             "print(f\"raw AIS file: {raw['length']:,} bytes, replication {raw['replication']}, block size {raw['blockSize'] // 2**20} MB, \"\n"
             "      f\"{-(-raw['length'] // raw['blockSize'])} blocks\")\n"
             "lake = get('http://localhost:9870/webhdfs/v1/maritime?op=GETCONTENTSUMMARY')['ContentSummary']\n"
             "print(f\"/maritime lake: {lake['fileCount']:,} files, {lake['length'] / 1e9:.1f} GB logical, {lake['spaceConsumed'] / 1e9:.1f} GB on disk (x replication)\")"),
    ("md", "## YARN: ResourceManager, NodeManagers and Spark applications"),
    ("code", "m = get('http://localhost:8088/ws/v1/cluster/metrics')['clusterMetrics']\n"
             "print('active NodeManagers', m['activeNodes'], '| memory', m['totalMB'], 'MB | vcores', m['totalVirtualCores'], '| apps completed', m['appsCompleted'])\n"
             "pd.DataFrame([{'node': n['id'], 'state': n['state'], 'memory_MB': n['usedMemoryMB'] + n['availMemoryMB'], 'containers': n['numContainers']}\n"
             "              for n in get('http://localhost:8088/ws/v1/cluster/nodes')['nodes']['node']])"),
    ("md", "YARN keeps finished applications in memory, so this list is empty after a ResourceManager restart; "
           "the `spark_master` column in the pipeline metrics below records where each stage ran."),
    ("code", "apps = (get('http://localhost:8088/ws/v1/cluster/apps')['apps'] or {}).get('app', [])\n"
             "if not apps: print('no applications since the last ResourceManager start')\n"
             "pd.DataFrame([{'id': a['id'], 'name': a['name'], 'state': a['state'], 'final': a['finalStatus'],\n"
             "               'minutes': round(a['elapsedTime'] / 60000, 1), 'MB-seconds': a['memorySeconds']} for a in apps])"),
    ("md", "## Spark standalone master and workers"),
    ("code", "sm = get('http://localhost:8080/json/')\nprint(sm['url'], sm['status'], 'alive workers:', sm['aliveworkers'], 'cores:', sm['cores'])\n"
             "pd.DataFrame([{'worker': w['id'], 'host': f\"{w['host']}:{w['port']}\", 'state': w['state'], 'cores': w['cores'], 'memory_MB': w['memory']} for w in sm['workers']])"),
    ("md", "## Pipeline metrics recorded by each Spark stage\n\nThese values are computed by Spark during the run "
           "and stored in HDFS (`/maritime/output/pipeline_metrics`) and MongoDB. `spark_master` shows where each stage ran."),
    ("code", "rows = []\nfor d in db.pipeline_metrics.find({}, {'_id': 0}):\n"
             "    rows.append({'stage': d['stage'], 'spark_master': d.get('spark_master'), 'elapsed_s': d.get('elapsed_seconds'), 'recorded_at': d.get('recorded_at')})\n"
             "pd.DataFrame(rows).sort_values('recorded_at')"),
    ("code", "c = db.pipeline_metrics.find_one({'stage': 'clean_ais'}, {'_id': 0})\n"
             "{k: c[k] for k in ('raw_rows', 'clean_rows', 'removed_rows', 'invalid_mmsi_rows', 'vessels', 'window_start_epoch', 'window_end_epoch')}"),
])

results = nb([
    ("md", "# MARITIME-X · 02 Results analysis\n\nReads the analytical results that the Spark pipeline wrote to MongoDB "
           "(compact, already aggregated). Every figure here is a view of computed results; nothing is typed in."),
    ("code", "import pandas as pd\nimport matplotlib.pyplot as plt\nfrom pymongo import MongoClient\n"
             "db = MongoClient('mongodb://localhost:27017')['maritime_x']\n"
             "plt.rcParams.update({'figure.dpi': 110, 'axes.spines.top': False, 'axes.spines.right': False})\n"
             "PRUSSIAN, MARINE, TEAL, AMBER, RED = '#003153', '#005B8F', '#2F8F8B', '#C88A27', '#B94A48'"),
    ("md", "## Port congestion (PCI, project-defined)"),
    ("code", "ports = pd.DataFrame(list(db.ports.find({'pci': {'$ne': None}}, {'_id': 0})))\n"
             "cols = ['port_name', 'pci', 'pci_band', 'arrivals', 'avg_queue', 'avg_waiting_h', 'avg_dwell_h', 'avg_turnaround_h', 'utilization']\n"
             "ports.sort_values('pci', ascending=False)[cols].head(12).round(2)"),
    ("code", "top = ports.sort_values('pci').tail(15)\nfig, ax = plt.subplots(figsize=(8, 5))\n"
             "ax.barh(top.port_name, top.avg_waiting_h, color=AMBER, label='avg waiting (h)')\n"
             "ax.barh(top.port_name, top.avg_dwell_h, left=top.avg_waiting_h, color=TEAL, label='avg berth time (h)')\n"
             "ax.set_xlabel('hours'); ax.set_title('Waiting and berth time, 15 most congested ports'); ax.legend(frameon=False)\nplt.tight_layout()"),
    ("md", "## Turnaround"),
    ("code", "v = pd.DataFrame(list(db.port_visits.find({'visit_type': 'BERTHED', 'complete': True}, {'_id': 0, 'turnaround_h': 1, 'waiting_h': 1, 'dwell_h': 1, 'vessel_group': 1})))\n"
             "print(len(v), 'complete berthed visits')\nv.groupby('vessel_group')[['waiting_h', 'dwell_h', 'turnaround_h']].describe().round(1).T"),
    ("md", "## AIS gap anomaly scores"),
    ("code", "g = pd.DataFrame(list(db.anomalies.find({}, {'_id': 0, 'risk_score': 1, 'risk_level': 1, 'gap_h': 1, 'transceiver': 1, 'vessel_group': 1, 'start_dist_to_port_nm': 1})))\n"
             "print(g.risk_level.value_counts().reindex(['Normal', 'Watch', 'Suspicious', 'High Priority']))\n"
             "fig, ax = plt.subplots(figsize=(8, 3.2))\nax.hist(g.risk_score, bins=range(0, 101, 5), color=MARINE, log=True)\n"
             "for x in (30, 60, 80): ax.axvline(x + .5, color=RED, lw=.8, ls='--')\nax.set_xlabel('risk score'); ax.set_ylabel('gaps (log)'); plt.tight_layout()"),
    ("code", "el = g[g.risk_score > 60]\nprint(f'elevated gaps: {len(el)}; share starting > 10 nm from a monitored port: {(el.start_dist_to_port_nm > 10).mean():.0%}')\n"
             "pd.crosstab(el.vessel_group, el.transceiver).assign(total=lambda d: d.sum(axis=1)).sort_values('total', ascending=False).head(10)"),
    ("md", "## AIS proximity / visibility events\n\nObservation only: the data contains AIS position reports, not radio messages."),
    ("code", "vis = pd.DataFrame(list(db.ais_visibility.find({}, {'_id': 0, 'visibility': 1, 'risk_level': 1, 'neighbours_total': 1, 'neighbours_near_during': 1})))\n"
             "pd.crosstab(vis.visibility, vis.risk_level, margins=True)"),
    ("md", "## MLlib KMeans gap clusters"),
    ("code", "m = db.ml_model.find_one({}, {'_id': 0})\nprint('k =', m['k'], '| silhouette by k:', {k: round(s, 3) for k, s in m['silhouette_by_k'].items()})\n"
             "pd.DataFrame(list(db.cluster_profiles.find({}, {'_id': 0, 'cluster': 1, 'size': 1, 'share': 1, 'avg_ml_score': 1, 'description': 1}))).round(3)"),
    ("md", "## Potential MMSI identity inconsistency"),
    ("code", "pd.DataFrame(list(db.identity_vessels.find({'flagged': True}, {'_id': 0, 'mmsi': 1, 'vessel_group': 1, 'conflict_events': 1, 'isolated_outliers': 1, 'max_implied_kn': 1}).sort('conflict_events', -1).limit(10)))"),
])

nbf.write(cluster, HERE / "01_cluster_and_pipeline.ipynb")
nbf.write(results, HERE / "02_results_analysis.ipynb")
print("written:", [p.name for p in HERE.glob("*.ipynb")])
