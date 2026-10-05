# Hadoop / Hive / Pig environment for the air-quality project.
# Usage:  source ~/air-quality-bda/env.sh

# Java 11 — Hadoop 3.3.x supports Java 8/11 only. Do NOT remove this export:
# the system default is a Java 21 JRE with no javac, which Hadoop cannot use.
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

export HADOOP_HOME=/home/pranav/hadoop
export HIVE_HOME=/home/pranav/hive
export PIG_HOME=/home/pranav/pig

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HIVE_CONF_DIR=$HIVE_HOME/conf
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME

export PIG_CLASSPATH="$PIG_HOME/conf:$HADOOP_CONF_DIR:$HIVE_CONF_DIR"

# PREPEND our Java and Hadoop toolchain so it wins over:
#   - /usr/bin/java                    (Java 21 JRE on this box)
#   - /mnt/c/Program Files/Common Files/Oracle/Java/javapath
#     (Windows' Java shim, which WSL2 imports into PATH and which points at
#      the Windows JDK 21 -- Hadoop must never pick that up)
# The Windows path entries cannot be removed from a sourced script's own PATH
# reliably, so prepending is the robust fix.
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$PIG_HOME/bin:$PATH"