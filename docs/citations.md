# Citations

## Dataset

> XKDR Forum (2026). *India Air Quality Database*. https://airquality.xkdr.org

**Sources of the underlying measurements:**

- Central Pollution Control Board (CPCB), Continuous Ambient Air Quality
  Monitoring (CAAQM) network — 553 stations, records from 2009.
- US Department of State air quality monitors, published via AirNow — 5 stations
  (New Delhi, Mumbai, Kolkata, Chennai, Hyderabad), from July 2019.

**Licence:** Creative Commons Attribution 4.0 (CC BY 4.0). The compilation may
be used for any purpose, including commercial, provided credit is given to the
India Air Quality Database, XKDR Forum, together with the sources above.

**Data as used in this project:**

| Property | Value |
|---|---|
| Rows | 46,937,170 |
| Stations | 538 |
| Pollutants analysed | PM2.5, PM10, NO2, SO2, CO, Ozone |
| Period | January – December 2024 |
| Export timestamp | 2026-09-30T11:25:11Z |
| Coverage of full archive | Jan 2009 – Mar 2026, 196,521,834 rows |

**No warranty.** The data is published as received from the monitoring networks,
which themselves label readings preliminary and not fully validated. It has not
been validated by XKDR for regulatory, legal or health decisions, and is not
suitable for those purposes. Check against the source network for anything that
matters.

## Software

| Component | Version | Licence | Reference |
|---|---|---|---|
| Apache Hadoop | 3.3.6 | Apache 2.0 | https://hadoop.apache.org/ |
| Apache Hive | 3.1.3 | Apache 2.0 | https://hive.apache.org/ |
| Apache Pig | 0.17.0 | Apache 2.0 | https://pig.apache.org/ |
| Apache Maven | 3.8.7 | Apache 2.0 | https://maven.apache.org/ |
| OpenJDK | 11, 8 | GPLv2+CPE | https://openjdk.org/ |
| Python | 3.12 | PSF | https://python.org/ |
| pandas | 3.0.6 | BSD-3 | https://pandas.pydata.org/ |
| NumPy | 2.5.3 | BSD-3 | https://numpy.org/ |
| PyArrow | 25.0.1 | Apache 2.0 | https://arrow.apache.org/ |
| scikit-learn | 1.9.1 | BSD-3 | https://scikit-learn.org/ |
| Matplotlib | 3.11.2 | PSF-based | https://matplotlib.org/ |
| seaborn | 0.13.2 | BSD-3 | https://seaborn.pydata.org/ |

## Compatibility references consulted

- Apache Hadoop Java version support — Apache Hadoop 3.3 and above supports Java 8
  and Java 11 (runtime only); Hadoop 3.5+ requires JDK 17 server-side.
  https://cwiki.apache.org/confluence/spaces/HADOOP/pages/100827883/
- Apache Hive and JDK 11 — [HIVE-25496](https://issues.apache.org/jira/browse/HIVE-25496).
  Hive 3.x's CLI casts the system classloader to `java.net.URLClassLoader`,
  removed in Java 9.
- Apache Hive manual installation — https://hive.apache.org/docs/latest/admin/manual-installation/
- Apache Pig 0.18.0 release notes (defaults to Hadoop 3, Tez 0.10, Hive 3) —
  https://pig.apache.org/releases.html
- XKDR Air Quality Database public API documentation —
  https://github.com/xKDR/Air-Quality-Database/blob/main/docs/PUBLIC_API.md
- WSL2 configuration (`.wslconfig`) — https://learn.microsoft.com/windows/wsl/wsl-config

## Environmental notes

- Microsoft, *Windows Subsystem for Linux documentation*, Microsoft Learn.
- Ubuntu 24.04 LTS package naming (Noble) — https://packages.ubuntu.com/

## Methodological basis

K-means follows the standard formulation (MacQueen, 1967) with Lloyd's
algorithm as implemented by scikit-learn. The choice of *k* uses both the elbow
method (inertia) and the mean silhouette coefficient (Rousseeuw, 1987), each
reported rather than one being silently preferred.

- MacQueen, J. (1967). Some methods for classification and clustering.
  *Proceedings of the 5th Berkeley Symposium on Mathematical Statistics and
  Probability*, 281–297.
- Rousseeuw, P. J. (1987). Silhouettes: a graphical aid to the interpretation
  and validation of cluster analysis. *Journal of Computational and Applied
  Mathematics*, 20, 53–65.

## Note on interpretation

The findings in `docs/results.md` are descriptive statistics of one year of
preliminary monitoring data. They are **not** a statement about air quality
health risk, regulatory compliance, or causation. The project performs no
prediction, forecasting or causal inference — its scope is descriptive
analytics over measured values.

Where a result coincides with patterns in the published literature (Delhi
highest among Indian cities, winter peak and monsoon trough, strong PM2.5–PM10
correlation), that is corroboration and not a source; every number reported
here was computed from the dataset by this pipeline.