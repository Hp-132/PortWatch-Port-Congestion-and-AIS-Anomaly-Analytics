# Environment for local runs from Git Bash / Linux:  source scripts/env_local.sh
export JAVA_HOME="${JAVA_HOME_17PLUS:-/c/Program Files/Java/jdk-24}"
export PYSPARK_PYTHON="$(command -v python)"
export MX_DATA_ROOT="output/lake" MX_RAW_INPUT="marine_ais_dataset.csv"
export MX_SPARK_MASTER="local[12]" MX_MONGO_URI="mongodb://localhost:27017"
