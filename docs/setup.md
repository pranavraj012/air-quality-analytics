# Setup

Exact commands to build the environment from scratch on WSL2 Ubuntu 24.04.

## 1. System packages

```bash
sudo apt-get update
sudo apt-get install -y openjdk-11-jdk-headless openjdk-8-jdk-headless maven libsnappy1v5
```

**`libsnappy1v5`, not `libsnappy1`.** Ubuntu 24.04 (Noble) renamed the package.
Asking for `libsnappy1` aborts the entire `apt-get install` transaction with
`E: Package 'libsnappy1' has no installation candidate`, and nothing else
installs either. Snappy itself is optional — Hadoop falls back to a pure-Java
compressor and only logs a warning — so it can be dropped entirely.

Verify:

```bash
java -version   # expect 21 (the system default; env.sh switches this)
javac -version  # expect 11.0.x
mvn -version    # expect 3.8.x
```

The box may already have a Java 21 JRE with **no compiler**. That is fine:
`env.sh` puts Java 11 first on `PATH`, and Hadoop needs `javac` only for the
Maven build.

## 2. Download the Hadoop ecosystem

```bash
mkdir -p ~/downloads && cd ~/downloads

wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
wget https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz
wget https://archive.apache.org/dist/pig/pig-0.17.0/pig-0.17.0.tar.gz
```

Total **~1.35 GB**.

### Verify the checksums

Do not skip this. Apache publishes a different algorithm per component:

```bash
# Hadoop: SHA512
wget -q -O- https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz.sha512
sha512sum hadoop-3.3.6.tar.gz

# Hive: SHA256
wget -q -O- https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz.sha256
sha256sum apache-hive-3.1.3-bin.tar.gz

# Pig (2017 vintage): MD5
wget -q -O- https://archive.apache.org/dist/pig/pig-0.17.0/pig-0.17.0.tar.gz.md5
md5sum pig-0.17.0.tar.gz
```

The `.sha512` files contain `SHA512(filename)=<hash>`; compare only the hash
part.

## 3. Extract

```bash
sudo mkdir -p /home/pranav/{hadoop,hive,pig}
cd ~/downloads

tar -xzf hadoop-3.3.6.tar.gz          -C /home/pranav/hadoop --strip-components=1
tar -xzf apache-hive-3.1.3-bin.tar.gz -C /home/pranav/hive   --strip-components=1
tar -xzf pig-0.17.0.tar.gz            -C /home/pranav/pig    --strip-components=1

# Reclaim the 1.35 GB of tarballs
rm -f ~/downloads/*.tar.gz
```

> **Write the Hadoop XML configs AFTER extracting.** The tarball ships its own
> default `core-site.xml` etc., and extracting overwrites anything written
> earlier. This cost an hour of confusion once — the symptom was Hadoop
> silently using `file:///` instead of HDFS.

## 4. Configuration

`env.sh` (in the project root) sets `JAVA_HOME` and every path. Source it:

```bash
. ./env.sh
```

Two ordering problems it solves:

1. WSL2 imports Windows' `Oracle/Java/javapath` into `PATH`, so plain `java`
   resolves to the Windows JDK 21. `env.sh` **prepends** `$JAVA_HOME/bin` rather
   than appending, because a sourced script cannot reliably strip entries from
   an already-assembled `PATH`.
2. Hadoop, Pig and Hive each need their own variables exported.

### Hadoop configuration

Files under `/home/pranav/hadoop/etc/hadoop/`:

- **`core-site.xml`** — `fs.defaultFS=hdfs://localhost:9000`
- **`hdfs-site.xml`** — `dfs.replication=1`, NameNode and DataNode directories
- **`mapred-site.xml`** — `mapreduce.framework.name=yarn`, plus the classpath
  and `JAVA_HOME` properties below
- **`yarn-site.xml`** — `memory-mb=4096`, `maximum-allocation-mb=4096`, shuffle
  aux-service, `vmem-check-enabled=false`

#### The two MapReduce settings that are easy to miss

On a single-node cluster the ApplicationMaster container is otherwise launched
with **no classpath at all**, and every job fails with:

```
Could not find or load main class org.apache.hadoop.mapreduce.v2.app.MRAppMaster
```

Fix with explicit classpaths:

```xml
<property>
  <name>yarn.app.mapreduce.am.classpath</name>
  <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
</property>
<property>
  <name>mapreduce.map.classpath</name>
  <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_CONF_DIR</value>
</property>
<property>
  <name>mapreduce.reduce.classpath</name>
  <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_CONF_DIR</value>
</property>
```

Those `$HADOOP_MAPRED_HOME` tokens only expand if the variable is visible
*inside* the container, which it is not by default:

```xml
<property>
  <name>yarn.app.mapreduce.am.env</name>
  <value>JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64,HADOOP_MAPRED_HOME=/home/pranav/hadoop</value>
</property>
<property>
  <name>mapreduce.map.env</name>
  <value>JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64,HADOOP_MAPRED_HOME=/home/pranav/hadoop</value>
</property>
<property>
  <name>mapreduce.reduce.env</name>
  <value>JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64,HADOOP_MAPRED_HOME=/home/pranav/hadoop</value>
</property>
```

Without `HADOOP_MAPRED_HOME` set this way Hadoop prints a helpful error naming
the missing property, and the job fails.

### Hive configuration

Two things are required, and Hive fails silently without both.

**1. Activate the log4j configs.** The tarball ships only `.template` files, so
errors vanish entirely and HiveServer2 just retries in a silent loop:

```bash
cd /home/pranav/hive/conf
for f in *.template; do cp "$f" "${f%.template}"; done
```

**2. Tell Hive where Hadoop is**, in `/home/pranav/hive/conf/hive-env.sh`.
Without this `hive classpath` returns an **empty string** and every command
fails before printing anything useful:

```bash
cat >> /home/pranav/hive/conf/hive-env.sh <<'EOF'

export HADOOP_HOME=/home/pranav/hadoop
export HIVE_CONF_DIR=/home/pranav/hive/conf
export HIVE_AUX_JARS_PATH=/home/pranav/hive/lib

export HADOOP_CLASSPATH=$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/yarn/lib/*

# Hive 3.1.3's CLI requires Java 8 (HIVE-25496). Hadoop runs on 11.
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
EOF
```

Verify: `hive classpath | wc -c` should print a few hundred characters, not `0`.

## 5. Hive metastore

```bash
cd /home/pranav/air-quality-bda
schematool -initSchema -dbType derby
```

Derby is embedded and file-based — fine for a single-node academic project. The
database is created at `./metastore_db`, so run this from the project root.

## 6. Start Hadoop

```bash
./start_all.sh
```

Uses `hdfs --daemon start` and `yarn --daemon start`, **not**
`start-dfs.sh` / `start-yarn.sh`. The wrappers SSH to `localhost` to launch each
daemon, and WSL2 has no SSH server:

```
localhost: ssh: connect to host localhost port 22: Connection refused
```

Verify:

```bash
hdfs dfsadmin -report   # Live datanodes: 1
yarn node -list         # 1 node, RUNNING
```

Then confirm MapReduce really works end to end — this catches the classpath
problems above immediately rather than 40 minutes into a real job:

```bash
hdfs dfs -mkdir -p /tmp/smoke
printf 'alpha\nbeta\nalpha\n' | hdfs dfs -put -f - /tmp/smoke/in.txt
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar \
    wordcount /tmp/smoke/in.txt /tmp/smoke/out
hdfs dfs -cat '/tmp/smoke/out/part-*'
# alpha 2
# beta 1
hdfs dfs -rm -r /tmp/smoke
```

## 7. Python environment

```bash
cd /home/pranav/air-quality-bda
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## 8. Dataset

The XKDR database is an **API**, not a download. Get a free key from
<https://airquality.xkdr.org/signup> and put it in `.env`:

```
XKDR_API_KEY=aqi_...
```

`.env` is gitignored. Without a key the scripts fall back to a public demo key
capped at 10,000 rows per query and restricted to 2024 bulk files.

```bash
python scripts/download_data.py --mode meta                      # check access
python scripts/download_data.py --mode bulk --year 2024 --months 1-12
```

Downloaded: **85 MB** of Parquet, 12 monthly files, 46,937,170 rows.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ClassCastException ... AppClassLoader cannot be cast to URLClassLoader` | Hive 3.1.3 CLI on Java 9+ | Set `JAVA_HOME` to Java 8 in `hive-env.sh` |
| `Could not find or load main class ...MRAppMaster` | AM container has no classpath | Add the three `*.classpath` properties to `mapred-site.xml` |
| `mapreduce.framework.name` error | `HADOOP_MAPRED_HOME` not set | Set it in `yarn.app.mapreduce.am.env` and the map/reduce equivalents |
| `ssh: connect to host localhost port 22: Connection refused` | `start-dfs.sh` needs SSH | Use `./start_all.sh` |
| Hive fails with no output | log4j templates never activated | `cp *.template` without the suffix in `hive/conf/` |
| `hive classpath` is empty | `HADOOP_CLASSPATH` unset | Add the exports to `hive-env.sh` |
| Java 21 after sourcing `env.sh` | Windows `javapath` precedes it in `PATH` | `env.sh` prepends; verify with `command -v java` |
| `libsnappy1` has no installation candidate | Ubuntu 24.04 package rename | Use `libsnappy1v5`, or drop it |
| Hadoop reads `file:///` instead of HDFS | configs overwritten by extraction | Rewrite `*-site.xml` after extracting |