# ⚓ MARITIME-X — PortWatch
### Big Data Analytics for Maritime Traffic, Port Congestion & AIS Anomaly Detection

**Turning 47.9 million vessel-position reports into actionable maritime insights.**

PortWatch is a maritime analytics application built on a distributed Hadoop and Spark data-processing pipeline. It transforms raw Automatic Identification System (AIS) data into vessel trajectories, port congestion indicators, turnaround metrics, and AIS reporting-gap risk assessments — all accessible through an interactive web dashboard.

The project combines **Hadoop HDFS, YARN, Apache Spark, machine learning, MongoDB, FastAPI, and MapLibre GL** to demonstrate how large-scale maritime data can be processed, analysed, and presented through a usable application.

## 🌊 The Application

PortWatch brings large-scale vessel analytics into one interface, making it easier to explore maritime traffic patterns and investigate unusual reporting behaviour.

### Dashboard Features

| Module | What you can explore |
|---|---|
| **Overview** | Fleet-wide KPIs, vessel activity timelines, and geographic vessel-position maps |
| **Port Congestion** | Port Congestion Index, queue estimates, waiting time, dwell time, and port utilisation |
| **Vessel Performance** | Search vessels, inspect vessel profiles, and explore historical trajectories and track playback |
| **AIS Anomalies** | Reporting gaps, risk categories, K-Means results, potential identity inconsistencies, and visibility around gaps |
| **Historical Trends** | Daily and hourly traffic patterns across vessel groups |

The frontend uses MapLibre GL for interactive geographic visualisation and ECharts for analytical charts. FastAPI retrieves prepared results from MongoDB, allowing the dashboard to focus on exploration rather than reprocessing the raw dataset.

**Application screenshots:** See [`screenshots/`](screenshots/).

## 🧭 Project at a Glance

| Component | Details |
|---|---|
| Dataset | 47,958,624 AIS reports across 17 columns |
| Data period | 1–7 January 2025 (UTC) |
| Raw data size | Approximately 4.9 GB |
| Vessel identifiers | 24,688 unique raw MMSIs |
| Processing | 12-stage Apache Spark pipeline |
| Distributed infrastructure | Hadoop HDFS + YARN, with Spark executors running on YARN |
| Data serving | MongoDB database: `maritime_x` |
| API | FastAPI REST endpoints |
| Visualisation | JavaScript, MapLibre GL, ECharts |
| Processing model | Batch analytics |
| Development environment | Windows host with three logical cluster nodes |

> **Data availability:** The complete raw AIS CSV is not committed to this repository. See [`dataset/README.md`](dataset/README.md) for dataset placement instructions.

## 🛠️ Technology Stack

| Layer | Technologies | Purpose |
|---|---|---|
| Data storage | Hadoop HDFS | Distributed storage, block management, and replication |
| Resource management | Hadoop YARN | Allocates cluster resources and launches application containers |
| Data processing | Apache Spark, PySpark | Distributed cleaning, trajectory reconstruction, aggregation, and analytics |
| Machine learning | Spark MLlib, K-Means | Groups AIS reporting-gap patterns for anomaly analysis |
| Data persistence | MongoDB | Stores processed collections and dashboard-ready results |
| Backend | Python, FastAPI | Exposes analytical results through REST APIs |
| Frontend | HTML, CSS, JavaScript | Builds the dashboard interface and application interactions |
| Geospatial visualisation | MapLibre GL | Maps vessel positions, port zones, and historical tracks |
| Charts | Apache ECharts | Displays KPIs, trends, distributions, and analytical comparisons |
| Testing | pytest | Tests project components and analytical logic |
| Configuration | YAML, CSV, PowerShell | Manages thresholds, port geofences, cluster configuration, and execution scripts |

## 🏗️ System Architecture

```text
                 RAW AIS DATASET
                47.96 Million Rows
                         |
                         v
                +------------------+
                |    Hadoop HDFS   |
                | Blocks & Replica |
                +------------------+
                         |
                         v
                +------------------+
                |    Hadoop YARN   |
                | Resource Manager |
                +------------------+
                         |
                         v
                +------------------+
                |    Apache Spark  |
                |  PySpark Pipeline|
                +------------------+
                         |
             +-----------+-----------+
             |           |           |
             v           v           v
          Cleaning   Trajectories  Analytics
                                   & K-Means
             |           |           |
             +-----------+-----------+
                         |
                         v
                +------------------+
                | Serving Datasets |
                |   & MongoDB      |
                +------------------+
                         |
                         v
                +------------------+
                |   FastAPI REST   |
                +------------------+
                         |
                         v
                +------------------+
                |   PortWatch UI   |
                | MapLibre + Charts|
                +------------------+
```

### Understanding the Hadoop/YARN Cluster

The project configures three **logical nodes**:

| Node | Main services | Responsibility |
|---|---|---|
| Master | HDFS NameNode, YARN ResourceManager, Spark Master | Tracks HDFS metadata and coordinates resource allocation |
| Worker 1 | HDFS DataNode, YARN NodeManager, Spark Worker | Stores data blocks and runs allocated processing containers |
| Worker 2 | HDFS DataNode, YARN NodeManager, Spark Worker | Stores replicated blocks and runs allocated processing containers |

The NameNode tracks where HDFS blocks are stored, while DataNodes hold the actual data. YARN's ResourceManager allocates resources, and each NodeManager manages local resources and launches containers on its worker. Spark executors perform the actual data-processing tasks.

**Infrastructure note:** These three nodes run as separate process groups on one 16 GB Windows machine, rather than on three separate physical computers. The setup demonstrates distributed processing mechanisms, but its performance should not be interpreted as production-cluster throughput.

## ⚙️ Data Processing Pipeline

The pipeline transforms the raw dataset into structured analytical outputs through 12 stages:

```text
01. clean_ais
       ↓
02. trajectories
       ↓
03. port_visits
       ↓
04. port_congestion
       ↓
05. turnaround
       ↓
06. gaps
       ↓
07. kmeans
       ↓
08. risk_scoring
       ↓
09. identity
       ↓
10. serving_views
       ↓
11. ais_visibility
       ↓
12. load_mongodb
```

| Stage | Purpose |
|---|---|
| Cleaning | Validates AIS records, handles invalid identifiers, and prepares usable observations |
| Trajectories | Reconstructs vessel movement from timestamped positions |
| Port visits | Identifies vessel activity within configured port zones |
| Congestion | Calculates queue, waiting, dwell, and utilisation indicators |
| Turnaround | Analyses vessel time spent during port calls |
| Gap detection | Identifies extended intervals between AIS reports |
| K-Means | Groups reporting-gap patterns for anomaly analysis |
| Risk scoring | Combines rule-based and clustering-derived signals into a prioritisation score |
| Identity analysis | Flags potentially inconsistent vessel movement or identity records |
| Serving views | Prepares datasets for efficient downstream access |
| AIS visibility | Adds geographic context around reporting gaps |
| MongoDB loading | Persists processed outputs for the dashboard API |

The pipeline supports stage-level execution and resumption. See [`pipeline/README.md`](pipeline/README.md) for the execution order, configuration, and recovery instructions.

## 📊 Results from the Full Run

The following figures are from the project's recorded full pipeline run.

| Metric | Result |
|---|---:|
| Raw AIS records processed | 47,958,624 |
| Clean AIS records | 47,912,404 |
| Records removed during cleaning | 46,220 |
| Vessels in cleaned registry | 24,529 |
| Port-zone position records | 2,094,457 |
| Identified port visits | 5,259 |
| AIS reporting gaps detected | 48,772 |
| Vessels associated with detected gaps | 11,328 |
| Potential identity inconsistencies | 593 |
| Correlation between rule-based and ML scores | 0.777 |

### Port Congestion Index

The project calculates a custom Port Congestion Index (PCI) to compare activity across 38 configured port zones.

| Port | PCI |
|---|---:|
| New Orleans | 84.3 |
| Houston | 83.4 |

These are project-defined comparative scores, not official port authority congestion ratings.

### AIS Reporting-Gap Risk Analysis

A reporting gap is an interval during which a vessel's expected AIS reports are absent. The project combines rule-based indicators with a K-Means-derived novelty signal to prioritise gaps for review.

The final risk categories include Normal, Watch, Suspicious, and High Priority. The dashboard provides context for investigation rather than automatically declaring a vessel illegal or malicious.

