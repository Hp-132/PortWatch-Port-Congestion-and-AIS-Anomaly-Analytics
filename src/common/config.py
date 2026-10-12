"""Central configuration loader.

Reads ``config/pipeline.yaml`` and applies environment-variable overrides so
the same code runs locally and, later, on the Hadoop/YARN cluster.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = Path(os.environ.get("MX_CONFIG", PROJECT_ROOT / "config" / "pipeline.yaml"))


@lru_cache(maxsize=1)
def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    env = os.environ
    cfg["storage"]["data_root"] = env.get("MX_DATA_ROOT", cfg["storage"]["data_root"])
    cfg["storage"]["raw_input"] = env.get("MX_RAW_INPUT", cfg["storage"]["raw_input"])
    cfg["spark"]["master"] = env.get("MX_SPARK_MASTER", cfg["spark"]["master"])
    cfg["spark"]["driver_memory"] = env.get("MX_DRIVER_MEMORY", cfg["spark"]["driver_memory"])
    if "MX_SHUFFLE_PARTITIONS" in env:
        cfg["spark"]["shuffle_partitions"] = int(env["MX_SHUFFLE_PARTITIONS"])
    cfg["mongodb"]["uri"] = env.get("MX_MONGO_URI", cfg["mongodb"]["uri"])
    cfg["mongodb"]["database"] = env.get("MX_MONGO_DB", cfg["mongodb"]["database"])
    return cfg


def resolve_uri(path_or_uri: str) -> str:
    """Turn a project-relative path into an absolute URI Spark can read.

    ``hdfs://`` / ``file:`` URIs and absolute paths are returned unchanged
    (absolute paths are converted to ``file:`` URIs).
    """
    if "://" in path_or_uri or path_or_uri.startswith("file:"):
        return path_or_uri
    p = Path(path_or_uri)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    return p.resolve().as_uri()
