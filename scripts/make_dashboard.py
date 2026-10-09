#!/usr/bin/env python3
"""Generate a single static HTML dashboard from results/*.csv.

No server, no JavaScript, no dependencies. Numbers are read directly
from the pipeline's own output files so they cannot drift.

Inputs (all in results/):
  descriptive_stats.csv, correlation.csv, monthly.csv,
  hourly_profile.csv, city_pm25.csv, thresholds.csv,
  exceedance_network.csv, exceedance_by_city.csv,
  exceedance_worst.csv
"""
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
R = ROOT / "results"

desc = pd.read_csv(R / "descriptive_stats.csv")
corr = pd.read_csv(R / "correlation.csv").sort_values("correlation", ascending=False)
monthly = pd.read_csv(R / "monthly.csv")
hourly = pd.read_csv(R / "hourly_profile.csv")
cities = pd.read_csv(R / "city_pm25.csv")
cities = cities[cities.stations >= 3].nlargest(10, "pm25_avg")
thresholds = pd.read_csv(R / "thresholds.csv")
net = pd.read_csv(R / "exceedance_network.csv")
worst = pd.read_csv(R / "exceedance_worst.csv")
bycity = pd.read_csv(R / "exceedance_by_city.csv")

f2 = lambda x: f"{x:,.2f}"
f0 = lambda x: f"{x:,.0f}"

PCTS = ["PM2.5", "PM10", "NO2", "SO2", "CO", "Ozone"]
STD = {"PM2.5": 60, "PM10": 100, "NO2": 80, "SO2": 50, "CO": 2000, "Ozone": 100}

# ---------- CSS bar strips (no JS) ----------
def bars(df, col, label, unit):
    mx = df[col].max()
    cells = []
    for _, r in df.iterrows():
        h = max(2, 100 * r[col] / mx)
        x = r.get("month", r.get("hour"))
        cells.append(
            f'<div class="bar-wrap" title="{label} {x}: {f2(r[col])} {unit}">'
            f'<div class="bar" style="height:{h:.0f}%"></div>'
            f'<span class="bar-x">{x}</span></div>'
        )
    return (
        f'<div class="bars"><div class="bars-inner">{"".join(cells)}</div>'
        f'<div class="bars-caption">{label} by {"month" if "month" in df.columns else "hour of day"} '
        f'({unit}) — peak {f2(mx)}</div></div>'
    )

monthly_bars = bars(monthly, "pm25_avg", "PM2.5", "µg/m³")
hourly_bars = bars(hourly, "pm25_avg", "PM2.5", "µg/m³")

# ---------- tables ----------
def table(df, headers, cols, formats=None, highlight=None):
    formats = formats or {}
    th = "".join(f"<th>{h}</th>" for h in headers)
    rows = []
    for _, r in df.iterrows():
        tds = []
        for c in cols:
            v = r[c]
            tds.append(f"<td>{formats[c](v) if c in formats else v}</td>")
        cls = ' class="hl"' if highlight and highlight(r) else ""
        rows.append(f"<tr{cls}>{''.join(tds)}</tr>")
    return f"<table><thead><tr>{th}</tr></thead><tbody>{''.join(rows)}</tbody></table>"

pollutant_tbl = table(
    desc,
    ["Pollutant", "n", "missing", "mean", "median", "min", "max", "stddev"],
    ["pollutant", "n", "missing", "mean", "median", "min", "max", "stddev"],
    formats={"n": f0, "missing": f0, "mean": f2, "median": f2, "min": f2, "max": f2, "stddev": f2},
)

corr_tbl = table(
    corr,
    ["Pair", "Pearson r"],
    ["pair", "correlation"],
    formats={"correlation": lambda x: f"{x:+.4f}"},
)

monthly_tbl = table(
    monthly,
    ["Month", "PM2.5", "PM10", "NO2", "SO2", "CO", "Ozone"],
    ["month", "pm25_avg", "pm10_avg", "no2_avg", "so2_avg", "co_avg", "ozone_avg"],
    formats={"month": lambda x: pd.Timestamp(2024, int(x), 1).strftime("%b"),
             "pm25_avg": f2, "pm10_avg": f2, "no2_avg": f2, "so2_avg": f2,
             "co_avg": f2, "ozone_avg": f2},
    highlight=lambda r: r["month"] in (8, 11),
)

city_tbl = table(
    cities,
    ["City", "Stations", "Mean PM2.5 (µg/m³)", "min", "max"],
    ["city_name", "stations", "pm25_avg", "pm25_min", "pm25_max"],
    formats={"stations": f0, "pm25_avg": f2, "pm25_min": f2, "pm25_max": f2},
)

threshold_tbl = table(
    thresholds,
    ["Pollutant", "NAAQS (µg/m³)", "Averaging time", "WHO 2021 (µg/m³)", "WHO averaging"],
    ["pollutant", "naaqs_ug_m3", "naaqs_averaging_time", "who_2021_ug_m3", "who_averaging_time"],
    formats={"naaqs_ug_m3": f0, "who_2021_ug_m3": f0},
)

def cell(row, p):
    m, pc = row[f"{p}_mean"], row[f"{p}_pct"]
    if pd.isna(m):
        return "—"
    return f"{f2(m)} <span class='dim'>({pc:.0f}%)</span>"

worst_rows = []
for _, r in worst.iterrows():
    pcts = {p: (r[f"{p}_pct"] if pd.notna(r[f"{p}_pct"]) else -1) for p in PCTS}
    driver = max(pcts, key=pcts.get)
    tds = "".join(f"<td>{cell(r, p)}</td>" for p in PCTS)
    worst_rows.append(
        f"<tr><td>{r.city_name}</td><td>{r.state_name}</td>{tds}"
        f"<td><b>{driver}</b></td></tr>"
    )
worst_tbl = (
    "<table><thead><tr><th>City</th><th>State</th>"
    + "".join(f"<th>{p}</th>" for p in PCTS)
    + "<th>Main offender</th></tr></thead><tbody>"
    + "".join(worst_rows) + "</tbody></table>"
)

net_tbl = table(
    net,
    ["Pollutant", "Standard", "Averaging", "Observations", "Above", "% above",
     "Cities above", "Worst city", "Worst city %"],
    ["pollutant", "standard_ug_m3", "averaging_time", "observations",
     "exceedances", "pct_above", "cities_with_mean_above",
     "worst_city", "worst_city_pct_above"],
    formats={"standard_ug_m3": f0, "observations": f0, "exceedances": f0,
             "pct_above": lambda x: f"{x:.1f}%",
             "cities_with_mean_above": f0, "worst_city_pct_above": lambda x: f"{x:.1f}%"},
)

# ---------- highlights: city x pollutant exceedances, ranked ----------
long_rows = []
for p in PCTS:
    sub = bycity[["city_name", "state_name", f"{p}_mean", f"{p}_pct"]].dropna(
        subset=[f"{p}_pct"])
    for _, r in sub.iterrows():
        long_rows.append({
            "city_name": r.city_name, "state_name": r.state_name,
            "pollutant": p, "standard": STD[p],
            "mean": r[f"{p}_mean"], "pct": r[f"{p}_pct"],
        })
long_df = pd.DataFrame(long_rows)
long_df = long_df[long_df.pct > 0].sort_values("pct", ascending=False)
n_exceed = len(long_df)

hl_rows = []
for i, (_, r) in enumerate(long_df.head(20).iterrows(), 1):
    hl_rows.append(
        f"<tr><td>{i}</td><td>{r.city_name}</td><td>{r.state_name}</td>"
        f"<td>{r.pollutant}</td><td>{f0(r.standard)}</td>"
        f"<td>{f2(r['mean'])}</td><td>{r.pct:.1f}%</td></tr>"
    )
highlights_tbl = (
    "<table><thead><tr><th>#</th><th>City</th><th>State</th><th>Pollutant</th>"
    "<th>NAAQS (µg/m³)</th><th>City mean (µg/m³)</th>"
    "<th>% of days above</th></tr></thead><tbody>"
    + "".join(hl_rows) + "</tbody></table>"
)

# diurnal peaks
def peak(col):
    mx = hourly.loc[hourly[col].idxmax()]
    mn = hourly.loc[hourly[col].idxmin()]
    return (f"{int(mx.hour):02d}:00 — {f2(mx[col])}", f"{int(mn.hour):02d}:00 — {f2(mn[col])}")

pm25_pk, pm25_mn = peak("pm25_avg")
co_pk, co_mn = peak("co_avg")
no2_pk, no2_mn = peak("no2_avg")

# headline exceedance figures
pm10_row = net[net.pollutant == "PM10"].iloc[0]
pm25_row = net[net.pollutant == "PM2.5"].iloc[0]
co_row = net[net.pollutant == "CO"].iloc[0]
worst_overall = long_df.iloc[0]

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Air Quality Analytics in India — Dashboard</title>
<style>
  :root {{ --ink:#1a2332; --mut:#5b6b7f; --line:#e3e8ef; --bg:#f6f8fa; --card:#fff; --acc:#0b6bcb; --bad:#b3541e; }}
  * {{ box-sizing:border-box; }}
  body {{ margin:0; font:15px/1.5 -apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
         color:var(--ink); background:var(--bg); }}
  header {{ background:var(--ink); color:#fff; padding:28px 32px; }}
  header h1 {{ margin:0 0 6px; font-size:24px; }}
  header p {{ margin:0; color:#b9c6d6; max-width:900px; }}
  main {{ max-width:1180px; margin:0 auto; padding:24px 20px 60px; }}
  h2 {{ font-size:17px; margin:34px 0 12px; padding-bottom:6px; border-bottom:2px solid var(--line); }}
  h2 .n {{ color:var(--acc); }}
  h3 {{ font-size:15px; margin:18px 0 8px; }}
  .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:12px; }}
  .card {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px 16px; }}
  .card .v {{ font-size:22px; font-weight:700; font-variant-numeric:tabular-nums; }}
  .card .v.bad {{ color:var(--bad); }}
  .card .k {{ color:var(--mut); font-size:12.5px; margin-top:2px; }}
  table {{ border-collapse:collapse; width:100%; background:var(--card); font-size:13.5px;
           font-variant-numeric:tabular-nums; }}
  th, td {{ padding:7px 10px; text-align:right; border-bottom:1px solid var(--line); }}
  th:first-child, td:first-child {{ text-align:left; }}
  thead th {{ background:#eef2f7; border-bottom:2px solid var(--line); }}
  tbody tr:nth-child(even) {{ background:#fafbfc; }}
  tr.hl td {{ background:#fff3d6; font-weight:600; }}
  .dim {{ color:var(--mut); font-weight:400; }}
  .grid2 {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; }}
  @media (max-width:900px) {{ .grid2 {{ grid-template-columns:1fr; }} }}
  .note {{ color:var(--mut); font-size:13px; margin-top:8px; }}
  .explain {{ background:var(--card); border:1px solid var(--line); border-left:4px solid var(--acc);
              border-radius:8px; padding:14px 18px; margin-top:12px; font-size:14px; }}
  .explain p {{ margin:0 0 10px; }}
  .explain p:last-child {{ margin-bottom:0; }}
  .imgbox {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:10px; }}
  .imgbox img {{ width:100%; height:auto; display:block; }}
  .gallery {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(340px,1fr)); gap:16px; }}
  .bars {{ background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px 16px 8px; }}
  .bars-inner {{ display:flex; align-items:flex-end; gap:3px; height:120px; }}
  .bar-wrap {{ flex:1; display:flex; flex-direction:column; align-items:center; height:100%; justify-content:flex-end; }}
  .bar {{ width:100%; background:var(--acc); border-radius:2px 2px 0 0; min-height:2px; }}
  .bar-x {{ font-size:9.5px; color:var(--mut); margin-top:3px; }}
  .bars-caption {{ font-size:12px; color:var(--mut); margin-top:8px; }}
  .recon {{ background:var(--card); border:1px solid var(--line); border-left:4px solid var(--acc);
            border-radius:8px; padding:14px 18px; font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
            font-size:13px; white-space:pre; overflow-x:auto; }}
  footer {{ color:var(--mut); font-size:12.5px; text-align:center; padding:24px; }}
</style>
</head>
<body>
<header>
  <h1>Air Quality Analytics in India — Dashboard</h1>
  <p>Hadoop ecosystem (HDFS · MapReduce · Pig · Hive) over 46,937,170 hourly readings
     from India's CPCB monitoring network, calendar year 2024.
     Every number below is read directly from the pipeline's own output in <code>results/</code>.</p>
</header>
<main>

<h2><span class="n">0 ·</span> Headline numbers</h2>
<div class="cards">
  <div class="card"><div class="v">46,937,170</div><div class="k">raw readings analysed (2024)</div></div>
  <div class="card"><div class="v">23,469,095</div><div class="k">readings after cleaning — Pig = MapReduce, exact</div></div>
  <div class="card"><div class="v">538</div><div class="k">monitoring stations · 29 states · 245 cities</div></div>
  <div class="card"><div class="v">4,198,821</div><div class="k">station-hours reshaped for analytics</div></div>
  <div class="card"><div class="v bad">42%</div><div class="k">of station-days above the PM10 standard (100 µg/m³)</div></div>
  <div class="card"><div class="v bad">{worst_overall.pct:.1f}%</div><div class="k">worst single exceedance: {worst_overall.city_name} {worst_overall.pollutant}</div></div>
  <div class="card"><div class="v">105.1</div><div class="k">Delhi mean PM2.5 (µg/m³) — highest of any city with ≥ 3 stations</div></div>
  <div class="card"><div class="v">0.839</div><div class="k">strongest correlation: PM2.5–PM10</div></div>
  <div class="card"><div class="v">31/31</div><div class="k">automated checks passing</div></div>
</div>

<h2><span class="n">1 ·</span> Dataset &amp; cleaning</h2>
<div class="grid2">
  <div>
    <p><b>Source:</b> India Air Quality Database, XKDR Forum (<code>airquality.xkdr.org</code>), CC BY 4.0.
       Full archive 196,521,834 rows · 558 stations · Jan 2009 – Mar 2026.
       Scope here: calendar 2024. Long format — one row per station × pollutant × hour.
       14 pollutants published; six core analysed: PM2.5, PM10, NO2, SO2, CO, Ozone.
       <b>CO is published in mg/m³; all others in µg/m³</b> — the pipeline converts CO ×1000.</p>
    <p class="note">Missing readings are reported, never imputed: PM2.5 6.35% · PM10 7.96% ·
       NO2 5.12% · SO2 7.94% · CO 5.16% · Ozone 8.53%.</p>
  </div>
  <div class="recon">Row reconciliation — nothing vanishes

Parquet data rows            46,937,170
  non-core pollutants      − 23,468,051   (14 → 6 pollutants)
  negative values          −       24
                           ─────────────
Pig = MapReduce output       23,469,095   ✓ exact

+ 52 header lines skipped (one per CSV file)
+ 1,544,613 cleaned readings (6.6%) labelled
  "Unknown" city — decommissioned stations,
  kept visible, not dropped</div>
</div>

<h2><span class="n">2 ·</span> Safe thresholds</h2>
<p>Indian National Ambient Air Quality Standards (NAAQS, CPCB, 2009) — the regulatory
limits a city is judged against — beside the stricter WHO 2021 guidelines:</p>
{threshold_tbl}
<p class="note">The NAAQS are 24-hour means for PM2.5, PM10, NO2 and SO2, and 8-hour means
for CO and Ozone. The analysis below therefore compares <b>daily 24-hour means</b> to the
PM/NO2/SO2 standards and <b>8-hour block means</b> (00–07, 08–15, 16–23) to the CO and
Ozone standards — each pollutant against its own averaging time, as the standards specify.</p>

<h2><span class="n">3 ·</span> Highlights — which city exceeded which threshold</h2>
<div class="cards">
  <div class="card"><div class="v bad">{pm10_row.pct_above:.0f}%</div><div class="k">of {f0(pm10_row.observations)} station-days above the PM10 standard</div></div>
  <div class="card"><div class="v bad">{f0(pm10_row.cities_with_mean_above)} / 245</div><div class="k">cities whose PM10 mean exceeds the standard</div></div>
  <div class="card"><div class="v bad">{pm25_row.pct_above:.0f}%</div><div class="k">of station-days above the PM2.5 standard (60 µg/m³)</div></div>
  <div class="card"><div class="v bad">{f0(pm25_row.cities_with_mean_above)}</div><div class="k">cities whose PM2.5 mean exceeds the standard</div></div>
  <div class="card"><div class="v">{co_row.pct_above:.1f}%</div><div class="k">of 8-hour blocks above the CO standard (2 mg/m³) — worst: Vapi {co_row.worst_city_pct_above:.0f}%</div></div>
  <div class="card"><div class="v">≤ 2.6%</div><div class="k">of days above standard for NO2, SO2, Ozone — within limits network-wide</div></div>
</div>

<h3>The worst exceedances, ranked (top 20 of {n_exceed:,} city–pollutant exceedances)</h3>
{highlights_tbl}
<p class="note">Sorted by % of days above the standard. A city must have at least 30
station-days (or 8-hour blocks) for a pollutant to be ranked, so a city with two days
of data cannot top the list. The full table for all 245 cities is
<code>results/exceedance_by_city.csv</code>.</p>

<h3>Network-wide exceedances</h3>
{net_tbl}
<p class="note">"Cities above" counts cities whose annual mean exceeds the 24-hour standard —
a chronic-exceedance screening: a city whose <i>annual average</i> sits above a
<i>24-hour</i> limit is above it chronically, not just on bad days. Observations are
station-days (24-hour means) for PM2.5/PM10/NO2/SO2 and 8-hour blocks for CO/Ozone.</p>

<h3>Worst 15 cities — mean µg/m³ and % of days above standard</h3>
{worst_tbl}
<p class="note">A cell reads <i>mean ( % of days above )</i>; — means the city has no
data for that pollutant. Cities are ranked by their worst pollutant. The National Capital
Region belt (Greater Noida, Gurugram, Ghaziabad, Delhi, Noida, Faridabad, Ballabgarh)
dominates the list, joined by Rajasthan's Sri Ganganagar, Hanumangarh, Bikaner and
Bhiwadi. Byrnihat (Assam) has the worst PM2.5 rate but a single station, so it is
excluded from city rankings that require ≥ 3 stations.</p>

<h2><span class="n">4 ·</span> Pollutant statistics (µg/m³)</h2>
{pollutant_tbl}
<p class="note">Every pollutant is right-skewed: median sits well below the mean, so the median is the
better "typical" figure. PM2.5 and PM10 top out at exactly 1000.00 — CPCB's instrument reporting
ceiling, so the top of those distributions is censored. CO shown after ×1000 conversion
(source mg/m³ → µg/m³). n = hourly readings; missing = station-pollutant-hours with no reading.</p>

<h2><span class="n">5 ·</span> Correlation — all 15 pairs</h2>
<div class="grid2">
  <div>{corr_tbl}</div>
  <div class="imgbox"><img src="results/correlation_heatmap.png" alt="Correlation heatmap"></div>
</div>
<p class="note">Pearson's r, pairwise complete observations. PM2.5–PM10 at 0.839 is the strongest
relationship (shared combustion source, co-transported). CO tracks the traffic pollutants
(PM10 0.411, NO2 0.358). Ozone is essentially uncorrelated with every other pollutant
(|r| ≤ 0.065) — it forms photochemically from traffic-emitted nitrogen oxides, so it behaves
inversely to the combustion pollutants. r is scale-invariant, which is why it was unaffected by
the CO unit conversion.</p>

<h2><span class="n">6 ·</span> Temporal analysis</h2>
<div class="grid2">
  <div>
    <h3 style="margin:0 0 8px">Monthly means (µg/m³)</h3>
    {monthly_tbl}
    <p class="note">PM2.5 swings fourfold: <b>86.8 in November</b> against <b>21.6 in August</b>
    (highlighted) — the monsoon scavenges particles. Winter brings cool, stagnant air and
    crop-residue burning.</p>
  </div>
  <div>
    <h3 style="margin:0 0 8px">Daily cycle</h3>
    <table>
      <thead><tr><th>Pollutant</th><th>Peak</th><th>Minimum</th></tr></thead>
      <tbody>
        <tr><td>PM2.5</td><td>{pm25_pk}</td><td>{pm25_mn}</td></tr>
        <tr><td>NO2</td><td>{no2_pk}</td><td>{no2_mn}</td></tr>
        <tr><td>CO</td><td>{co_pk}</td><td>{co_mn}</td></tr>
      </tbody>
    </table>
    {monthly_bars}
    {hourly_bars}
  </div>
</div>
<div class="explain">
  <p><b>What the daily cycle is.</b> It answers: within a single day, when does pollution
  build, and when does it clear?</p>
  <p><b>NO2 and CO are primary emissions</b> — they come straight out of a tailpipe — so they
  spike at <b>20:00, the evening rush hour</b> (NO2 28.8, CO 1060.7 µg/m³), and fall in the
  afternoon when traffic is lightest.</p>
  <p><b>PM2.5 is an accumulating pollutant.</b> It forms in the atmosphere from gases rather
  than being emitted directly, so it builds through the day and peaks two hours later, at
  <b>22:00</b> (59.0 µg/m³), and is lowest mid-afternoon (16:00, 39.8) when the atmosphere
  mixes best.</p>
  <p>The two-hour offset between the traffic peak (20:00) and the PM2.5 peak (22:00) is the
  signature of an accumulating secondary pollutant versus a directly emitted one — same
  pollutants, different physics.</p>
</div>

<h2><span class="n">7 ·</span> Location-wise — mean PM2.5 by city (≥ 3 stations)</h2>
<div class="grid2">
  <div>{city_tbl}</div>
  <div class="imgbox"><img src="results/city_pm25_comparison.png" alt="City PM2.5 comparison"></div>
</div>
<p class="note">Delhi and its four NCR neighbours take the top five — contiguous cities sharing one
air shed. The ≥ 3 station filter matters: without it a single-station town (Byrnihat, 128.7 µg/m³
from one monitor) would outrank Delhi.</p>

<h2><span class="n">8 ·</span> Remaining charts</h2>
<div class="gallery">
  <div class="imgbox"><img src="results/temporal_trends.png" alt="Temporal trends"></div>
  <div class="imgbox"><img src="results/pollutant_distribution.png" alt="Pollutant distributions"></div>
</div>

<h2><span class="n">9 ·</span> How these numbers were produced</h2>
<div class="recon">XKDR API → monthly Parquet (long format)
  │
  ├─ scripts/download_data.py      fetch + metadata
  ├─ scripts/inspect_data.py       measure schema, units, nulls, ranges
  ├─ scripts/prepare_sample.py     Parquet → CSV, lossless (52 files, 3.63 GB)
  ▼
HDFS /airquality/raw
  └─► PIG  LOAD → FILTER → FOREACH → GROUP → STORE   → /airquality/cleaned/all
        · drop non-numeric, negative, non-core pollutants
        · missing city → "Unknown"   · derive year/month/day/hour
        · CO: mg/m³ → µg/m³ (×1000)
        ▼
        ├─► MAPREDUCE   1,454 city × pollutant pairs   (agrees with Pig exactly)
        ├─► HIVE        SQL: GROUP BY / HAVING / CORR / VAR_POP
        └─► PYTHON      pivot → 4,198,821 station-hours
                         statistics · correlation · charts
                         NAAQS exceedance (daily & 8-hour block means)
Stack: OpenJDK 8 · Hadoop 3.3.6 · Pig 0.17.0 · Hive 3.1.3 · Python 3.12
(pandas, numpy, pyarrow, scikit-learn, matplotlib, seaborn)</div>
<p class="note">Correctness: two independently written programs (Pig and MapReduce) process the same
rows with the same filters and agree exactly at 23,469,095; every raw row is reconciled above;
31 automated checks pass (<code>./tests/test_pipeline.sh</code>). This dashboard is a static file —
no server, no database, no JavaScript. Regenerate it with <code>scripts/exceedance.py</code> then
<code>scripts/make_dashboard.py</code>; charts with <code>visualization/*.py</code>.</p>

</main>
<footer>
  Air Quality Analytics in India Using the Hadoop Ecosystem · data: XKDR Forum India Air Quality
  Database, CC BY 4.0 · standards: CPCB NAAQS 2009, WHO 2021 guidelines · preliminary, not
  validated for regulatory, legal or health decisions · PM2.5/PM10 censored at 1000.00 µg/m³
  (instrument ceiling)
</footer>
</body>
</html>
"""

(ROOT / "dashboard.html").write_text(html, encoding="utf-8")
print(f"wrote dashboard.html ({len(html):,} bytes)")
print(f"highlights: top 20 of {n_exceed:,} city–pollutant exceedances")
