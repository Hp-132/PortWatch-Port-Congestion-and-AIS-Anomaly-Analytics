# Environment for running MARITIME-X on the 3-node cluster (PowerShell). Dot-source it:
#   . .\scripts\env_cluster.ps1
# The driver (this machine's Python) submits to YARN; executors run in YARN
# containers on worker1/worker2; all data is read from / written to HDFS.
$Project = Split-Path $PSScriptRoot -Parent
$env:HADOOP_HOME = "C:\hadoop-3.3.6"
$env:HADOOP_CONF_DIR = "$Project\cluster\conf\master"     # client config: NameNode + ResourceManager addresses
$env:YARN_CONF_DIR = $env:HADOOP_CONF_DIR
# Spark 4.x needs Java 17+; C:\Java\jdk-24 is a junction (no spaces, so YARN launch scripts work)
$env:JAVA_HOME = "C:\Java\jdk-24"
$rest = (($env:Path -split ";") | Where-Object { $_ -and ($_ -notmatch "\\Java\\|jdk") }) -join ";"
$env:Path = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$rest"
$env:PYSPARK_PYTHON = (Get-Command python).Source
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON

$env:MX_SPARK_MASTER = "yarn"
$env:MX_DATA_ROOT = "hdfs://localhost:9820/maritime"
$env:MX_RAW_INPUT = "hdfs://localhost:9820/maritime/raw/ais/marine_ais_dataset.csv"
$env:MX_MONGO_URI = "mongodb://localhost:27017"
Write-Host "MARITIME-X cluster env: master=yarn lake=$env:MX_DATA_ROOT conf=$env:HADOOP_CONF_DIR"
