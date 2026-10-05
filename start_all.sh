#!/usr/bin/env bash
# Start HDFS and YARN daemons for the air-quality project.
#
# Uses `hdfs --daemon start` / `yarn --daemon start` rather than the
# start-dfs.sh / start-yarn.sh wrappers. The wrappers SSH to localhost to
# launch each daemon, and there is no SSH server in this WSL2 environment.
#
# Usage:  ./start_all.sh

set -euo pipefail

. "$(dirname "$0")/env.sh"

echo "Starting HDFS..."
hdfs --daemon start namenode
sleep 5
hdfs --daemon start datanode
sleep 5
hdfs --daemon start secondarynamenode
sleep 3

echo "Starting YARN..."
yarn --daemon start resourcemanager
sleep 5
yarn --daemon start nodemanager
sleep 5

# The JobHistory Server is not optional in practice. Pig queries it on port
# 10020 for job statistics AFTER a job has already succeeded; without it Pig
# retries ten times and stalls, adding minutes of dead time to every run
# even though the results were written correctly.
echo "Starting JobHistory Server..."
mapred --daemon start historyserver
sleep 5

echo
echo "HDFS:"
hdfs dfsadmin -report | head -4
echo
echo "YARN:"
yarn node -list 2>/dev/null | grep -E "Total Nodes|RUNNING"
echo
echo "Hadoop is up. Web UIs: NameNode http://localhost:9870, YARN http://localhost:8088"