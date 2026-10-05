#!/usr/bin/env bash
# Stop the Hadoop daemons.
#
# Run this when finished. The daemons are long-lived JVMs that together hold
# several GB of WSL2 memory; leaving them running keeps that memory reserved
# and slows Windows down.
#
# Usage:  ./stop_all.sh

set -uo pipefail

. "$(dirname "$0")/env.sh"

echo "Stopping YARN..."
mapred --daemon stop historyserver 2>&1 | grep -v "^WARNING" || true
yarn --daemon stop resourcemanager 2>&1 | grep -v "^WARNING" || true
yarn --daemon stop nodemanager 2>&1 | grep -v "^WARNING" || true

echo "Stopping HDFS..."
hdfs --daemon stop secondarynamenode 2>&1 | grep -v "^WARNING" || true
hdfs --daemon stop datanode 2>&1 | grep -v "^WARNING" || true
hdfs --daemon stop namenode 2>&1 | grep -v "^WARNING" || true

echo
echo "Hadoop stopped. To return memory to Windows, run 'wsl --shutdown' in PowerShell."