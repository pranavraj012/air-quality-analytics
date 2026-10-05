#!/usr/bin/env bash
# Start the MapReduce JobHistory Server.
#
# Pig submits jobs to YARN and then queries the history server on port 10020
# to collect job statistics. Without it, Pig retries the connection 10 times
# after a job has ALREADY SUCCEEDED, which added several minutes of dead time
# to every Pig run and left the calling script waiting on output that was
# never going arrive cleanly.
#
# The job itself is unaffected: results are written to HDFS either way. This
# only stops Pig from stalling on statistics it cannot fetch.
#
# Usage:  ./start_historyserver.sh

set -uo pipefail

. "$(dirname "$0")/env.sh"

if ss -ltn 2>/dev/null | grep -q ':10020'; then
    echo "JobHistory Server already listening on 10020."
    exit 0
fi

# mapred-site.xml must name the history server, or YARN containers get no
# MR-JHS address and Pig's statistics query still fails.
HADOOP_MAPRED_HOME="$HADOOP_MAPRED_HOME" \
mapred --daemon start historyserver

sleep 6

if ss -ltn 2>/dev/null | grep -q ':10020'; then
    echo "JobHistory Server started on port 10020."
    echo "Web UI: http://localhost:19888"
else
    echo "WARNING: history server did not start; Pig will still run but"
    echo "         will stall briefly collecting job statistics."
fi