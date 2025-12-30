#!/bin/bash

# Script to run only Spark Processing Server in WSL
echo "Starting Spark Processing Server in WSL..."

# Change to project directory
cd ~/bigdata_project

# Set JAVA_HOME
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
echo "Using Java: $JAVA_HOME"

# Activate virtual environment
source venv/bin/activate

# Install compatible PySpark version
pip uninstall -y pyspark
pip install pyspark==3.4.3

echo "Starting Spark Processing Server only..."
python spark_processing_server.py