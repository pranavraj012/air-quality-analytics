#!/usr/bin/env python3
"""Per-city, per-pollutant exceedance of Indian NAAQS safe thresholds.

Standards (CPCB National Ambient Air Quality Standards, 2009
notification, Ministry of Environment, Forest and Climate Change):
  PM2.5  60 µg/m³   24-hour mean
  PM10  100 µg/m³   24-hour mean
  NO2    80 µg/m³   24-hour mean
  SO2    50 µg/m³   24-hour mean
  CO    2000 µg/m³   8-hour mean  (2 mg/m³)
  Ozone 100 µg/m³   8-hour mean

Method, matching each standard's averaging time:
  · PM2.5/PM10/NO2/SO2 -> 24-hour (daily) means per station
  · CO/Ozone            -> 8-hour block means per station
                          (blocks 00-07, 08-15, 16-23)
A station-day (or block) counts as exceeding when its mean is above
the standard. Cities are ranked only when they have >= 30
observations for a pollutant, so a city with two days of data
cannot top the list.

Outputs (results/):
  thresholds.csv            the standards themselves
  exceedance_network.csv    network-wide summary per pollutant
  exceedance_by_city.csv    every city x pollutant: mean and % above
  exceedance_worst.csv      top 15 cities by worst exceedance
"""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
WIDE = ROOT / "data/processed/cleaned_wide.csv"
R = ROOT / "results"

# column -> (display name, NAAQS standard µg/m³, averaging time,
#            WHO 2021 guideline µg/m³, WHO averaging time)
POLLUTANTS = {
    "pm25_ug_m3":   ("PM2.5",  60,    "24-hour", 15,  "24-hour"),
    "pm10_ug_m3":   ("PM10",  100,    "24-hour", 45,  "24-hour"),
    "no2_ug_m3":    ("NO2",    80,    "24-hour", 25,  "24-hour"),
    "so2_ug_m3":    ("SO2",    50,    "24-hour", 40,  "24-hour"),
    "co_ug_m3":     ("CO",   2000,    "8-hour",  4000, "24-hour"),
    "ozone_ug_m3":  ("Ozone", 100,    "8-hour",  100, "8-hour"),
}

MIN_OBS = 30  # a city needs this many station-days/blocks to be ranked

print(f"reading {WIDE}")
df = pd.read_csv(
    WIDE,
    usecols=[
        "station_id", "collected_at", "hour",
        "pm25_ug_m3", "pm10_ug_m3", "no2_ug_m3",
        "so2_ug_m3", "co_ug_m3", "ozone_ug_m3",
        "state_name", "city_name",
    ],
)
print(f"station-hours: {len(df):,}")

df["date"] = df["collected_at"].str.slice(0, 10)
df["block"] = df["hour"] // 8
keys = ["station_id", "city_name", "state_name", "date"]

poll_cols = list(POLLUTANTS)
hourly_city = df.groupby(["city_name", "state_name"], observed=True)[poll_cols].mean()

rows_net = []
city_parts = {}

for col, (name, std, window, who, who_window) in POLLUTANTS.items():
    if window == "24-hour":
        cols24 = [c for c in poll_cols if POLLUTANTS[c][2] == "24-hour"]
        daily = df.groupby(keys, observed=True)[cols24].mean()
        tbl = daily.reset_index()
        unit = "station-days"
    else:
        cols8 = [c for c in poll_cols if POLLUTANTS[c][2] == "8-hour"]
        blocks = df.groupby(keys + ["block"], observed=True)[cols8].mean()
        tbl = blocks.reset_index()
        unit = "8-hour blocks"

    tbl["over"] = tbl[col] > std

    per_city = (
        tbl.groupby(["city_name", "state_name"], observed=True)
        .agg(n=(col, "count"), mean=(col, "mean"), over=("over", "sum"))
    )
    per_city["pct"] = 100.0 * per_city["over"] / per_city["n"]
    per_city = per_city[per_city["n"] >= MIN_OBS]

    city_parts[col] = per_city

    n_total = int(tbl[col].count())
    n_over = int(tbl["over"].sum())
    cities_above = int((hourly_city[col] > std).sum())
    worst = per_city.sort_values("pct", ascending=False)
    worst_city = worst.index[0] if len(worst) else ("—", "—")
    worst_pct = float(worst["pct"].iloc[0]) if len(worst) else float("nan")

    rows_net.append({
        "pollutant": name,
        "standard_ug_m3": std,
        "averaging_time": window,
        "who_2021_ug_m3": who,
        "who_averaging_time": who_window,
        "observations": n_total,
        "observation_unit": unit,
        "exceedances": n_over,
        "pct_above": 100.0 * n_over / n_total if n_total else float("nan"),
        "cities_with_mean_above": cities_above,
        "worst_city": worst_city[0],
        "worst_city_pct_above": worst_pct,
    })

# ---------- per-city wide table ----------
base = None
for col, (name, *_rest) in POLLUTANTS.items():
    pc = city_parts[col].copy()
    pc.columns = [f"{name}_n", f"{name}_mean", f"{name}_over", f"{name}_pct"]
    base = pc if base is None else base.join(pc, how="outer")
base = base.reset_index()
base.to_csv(R / "exceedance_by_city.csv", index=False)
print(f"exceedance_by_city.csv : {len(base):,} cities")

# ---------- top 15 by worst exceedance ----------
worst = (
    base.set_index(["city_name", "state_name"])
    .filter(regex="_pct$")
    .max(axis=1)
    .sort_values(ascending=False)
)
top = worst.head(15).index
top_df = base.set_index(["city_name", "state_name"]).loc[top].reset_index()
top_df.to_csv(R / "exceedance_worst.csv", index=False)
print(f"exceedance_worst.csv   : {len(top_df)} cities")

net = pd.DataFrame(rows_net)
net.to_csv(R / "exceedance_network.csv", index=False)

thr = pd.DataFrame(
    [
        {
            "pollutant": name,
            "naaqs_ug_m3": std,
            "naaqs_averaging_time": window,
            "who_2021_ug_m3": who,
            "who_averaging_time": who_window,
        }
        for col, (name, std, window, who, who_window) in POLLUTANTS.items()
    ]
)
thr.to_csv(R / "thresholds.csv", index=False)

print()
print(net[["pollutant", "standard_ug_m3", "pct_above",
           "cities_with_mean_above", "worst_city",
           "worst_city_pct_above"]].to_string(index=False))
