"""Logical data-lake layout.

The layout mirrors the HDFS structure used on the cluster::

    /maritime/raw/ais
    /maritime/processed/{clean_ais, vessel_registry, vessel_tracks, port_visits, ais_gaps}
    /maritime/analytics/{port_congestion, turnaround, dark_vessel}
    /maritime/ml/{features, models}
    /maritime/output

Locally ``data_root`` is ``output/lake``; on the cluster it is
``hdfs://master:9000/maritime``. Jobs only ever call :func:`lake_path`.
"""
from __future__ import annotations

from .config import load_config, resolve_uri

LAYOUT = {
    # processed
    "clean_ais": "processed/clean_ais",
    "vessel_registry": "processed/vessel_registry",
    "vessel_tracks": "processed/vessel_tracks",
    "port_points": "processed/port_points",
    "port_visits": "processed/port_visits",
    "ais_gaps": "processed/ais_gaps",
    # analytics
    "congestion_hourly": "analytics/port_congestion/hourly",
    "congestion_daily": "analytics/port_congestion/daily",
    "port_summary": "analytics/port_congestion/port_summary",
    "turnaround_visits": "analytics/turnaround/visits",
    "turnaround_stats": "analytics/turnaround/stats",
    "gap_risk": "analytics/dark_vessel/gap_risk",
    "identity_events": "analytics/dark_vessel/identity_events",
    "identity_vessels": "analytics/dark_vessel/identity_vessels",
    "cluster_profiles": "analytics/dark_vessel/cluster_profiles",
    "ais_visibility": "analytics/dark_vessel/ais_visibility",
    # ml
    "gap_features": "ml/features/gap_features",
    "gap_ml_scores": "ml/features/gap_ml_scores",
    "kmeans_model": "ml/models/gap_kmeans",
    # serving outputs (map-ready, small)
    "vessel_summary": "output/vessel_summary",
    "vessel_positions": "output/vessel_positions",
    "vessel_track_points": "output/vessel_track_points",
    "daily_trends": "output/daily_trends",
    "daily_group_activity": "output/daily_group_activity",
    "hourly_activity": "output/hourly_activity",
    "pipeline_metrics": "output/pipeline_metrics",
}


def data_root() -> str:
    return resolve_uri(load_config()["storage"]["data_root"]).rstrip("/")


def lake_path(name: str) -> str:
    return f"{data_root()}/{LAYOUT[name]}"


def raw_input() -> str:
    return resolve_uri(load_config()["storage"]["raw_input"])
