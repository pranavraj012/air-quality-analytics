# Hadoop / Hive / Pig environment for the air-quality project.
# Usage:  source ~/air-quality-bda/env.sh

# Java 8 — the single JVM for the entire stack.
#
# Java 8 is required, not preferred:
#   - Hive 3.1.3's CLI casts the system classloader to java.net.URLClassLoader,
#     which was removed in Java 9 (Apache HIVE-25496). Hive cannot start on 11.
#   - Hive's MapReduce containers load Kryo 3.0.3, which reflects on
#     java.util.ArrayList internals that moved in Java 9, failing with
#     "NoSuchFieldException: parentOffset".
#   - Hadoop 3.3.6 supports Java 8 and 11, so Java 8 costs nothing there.
#   - The MapReduce job targets Java 8 bytecode (maven.compiler.target=8), so
#     Maven builds and runs it under this same JDK.
#
# The system default is a Java 21 JRE with no javac, which no Hadoop component
# can use. Java 11 is no longer needed by anything in this project and can be
# removed with: sudo apt-get remove openjdk-11-jdk-headless
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

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