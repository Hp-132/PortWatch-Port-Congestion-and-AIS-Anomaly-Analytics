"""SparkSession factory and CSV I/O helpers shared by every job."""
from __future__ import annotations

import os
import sys

from pyspark.sql import DataFrame, SparkSession

from .config import load_config
from .paths import lake_path


def get_spark(job_name: str) -> SparkSession:
    cfg = load_config()["spark"]
    # Executors must use the same interpreter as the driver in local mode.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    builder = (
        SparkSession.builder.appName(f"{cfg['app_name']} :: {job_name}")
        .config("spark.sql.session.timeZone", cfg["timezone"])
        .config("spark.sql.shuffle.partitions", cfg["shuffle_partitions"])
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.ui.showConsoleProgress", "false")
    )
    master = cfg["master"]
    if master:
        builder = builder.master(master)
    if master.startswith("local"):
        # In local mode the driver hosts the executors, so give it the memory.
        builder = builder.config("spark.driver.memory", cfg["driver_memory"])
    else:
        builder = _cluster_config(builder, master, cfg["cluster"])
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if not master.startswith("local"):
        # Executors import src.* (e.g. the MongoDB partition writer) from this zip.
        spark.sparkContext.addPyFile(_src_zip())
    return spark


def _cluster_config(builder, master: str, c: dict):
    """Executor sizing and runtime environment for YARN / Spark standalone."""
    py = os.environ.get("PYSPARK_PYTHON", sys.executable)
    builder = (builder
               .config("spark.driver.memory", c["driver_memory"])
               .config("spark.executor.memory", c["executor_memory"])
               .config("spark.executor.cores", c["executor_cores"])
               .config("spark.executorEnv.PYSPARK_PYTHON", py)
               .config("spark.executorEnv.JAVA_HOME", c["executor_java_home"]))
    if master == "yarn":
        builder = (builder
                   .config("spark.submit.deployMode", "client")
                   .config("spark.executor.instances", c["executor_instances"])
                   .config("spark.executor.memoryOverhead", c["executor_memory_overhead"])
                   .config("spark.yarn.am.memory", "512m")
                   .config("spark.yarn.appMasterEnv.JAVA_HOME", c["executor_java_home"])
                   .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", py)
                   .config("spark.yarn.stagingDir", c["yarn_staging_dir"]))
        if c.get("yarn_jars"):
            # Spark jars pre-staged in HDFS, so each application does not upload ~300 MB.
            builder = builder.config("spark.yarn.jars", c["yarn_jars"])
    else:  # spark://host:port standalone
        builder = builder.config("spark.cores.max", int(c["executor_cores"]) * int(c["executor_instances"]))
    return builder


def _src_zip() -> str:
    """Zip the ``src`` package (fresh on every run) for executors."""
    import tempfile
    import zipfile
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    out = Path(tempfile.gettempdir()) / "maritime_x_src.zip"
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for f in (root / "src").rglob("*.py"):
            z.write(f, f.relative_to(root).as_posix())
    return str(out)


def _csv_options() -> dict:
    comp = load_config()["storage"]["csv_compression"]
    opts = {"header": "true", "timestampFormat": "yyyy-MM-dd HH:mm:ss"}
    if comp and comp != "none":
        opts["compression"] = comp
    return opts


def write_csv(df: DataFrame, name: str, partition_by: list[str] | None = None,
              coalesce: int | None = None) -> str:
    """Write a DataFrame to the lake as headered CSV (overwrite)."""
    path = lake_path(name)
    if coalesce:
        df = df.coalesce(coalesce)
    writer = df.write.mode("overwrite").options(**_csv_options())
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.csv(path)
    return path


def read_csv(spark: SparkSession, name: str, schema=None) -> DataFrame:
    """Read a lake CSV dataset. Pass an explicit schema for large datasets so
    Spark does not need an extra inference pass."""
    reader = spark.read.options(**_csv_options())
    if schema is not None:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", "true")
    return reader.csv(lake_path(name))
