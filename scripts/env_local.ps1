# Environment for LOCAL development runs (PowerShell). Dot-source it:
#   . .\scripts\env_local.ps1
# Spark 4.x needs Java 17+. Adjust JAVA_HOME if your JDK lives elsewhere.
$env:JAVA_HOME = "C:\Program Files\Java\jdk-24"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
# winutils.exe (Hadoop native helpers for Windows) - required by Spark on Windows
if (-not $env:HADOOP_HOME) { $env:HADOOP_HOME = "C:\hadoop-3.3.6" }
$env:PYSPARK_PYTHON = (Get-Command python).Source
# Local lake (mirrors the HDFS layout) and local MongoDB
$env:MX_DATA_ROOT = "output/lake"
$env:MX_RAW_INPUT = "marine_ais_dataset.csv"
$env:MX_SPARK_MASTER = "local[12]"
$env:MX_MONGO_URI = "mongodb://localhost:27017"
Write-Host "MARITIME-X local env: JAVA_HOME=$env:JAVA_HOME master=$env:MX_SPARK_MASTER lake=$env:MX_DATA_ROOT"
