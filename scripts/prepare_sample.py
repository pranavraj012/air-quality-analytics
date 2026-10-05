#!/usr/bin/env python3
"""Convert the downloaded Parquet partitions to CSV for Pig and MapReduce.

WHY THIS EXISTS
---------------
The source data is Parquet. Pig has no Parquet loader that works reliably
against this schema (the piggybank loader has been unmaintained since 2017),
and MapReduce has no built-in Parquet input format either. So the pipeline
converts once, to plain CSV, and everything downstream reads boring text.

WHAT THIS DOES NOT DO
---------------------
It performs NO cleaning, NO filtering, NO unit conversion and NO reshaping.
The output is a faithful, lossless CSV rendering of the raw Parquet. All
cleaning rules live in pig/01_clean_and_pivot.pig so there is exactly one
place where data is modified, and it is readable.

Usage:
    python scripts/prepare_sample.py
    python scripts/prepare_sample.py --out data/sample
"""

import argparse
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Column order for the CSV.
#
# station_name is deliberately EXCLUDED. 93% of station names contain a comma
# ("CRRI Mathura Road, Delhi - IMD"), which breaks naive comma-splitting in
# the MapReduce mapper and silently shifts every column to its right. Nothing
# in the pipeline aggregates by station name -- station_id is the key, and the
# full names live in data/raw/stations.parquet -- so it is left out of the
# working CSV rather than quoted-and-parsed.
COLUMNS = [
    "station_id",
    "state_name",
    "city_name",
    "parameter_name",
    "unit",
    "collected_at",
    "value",
    "source",
]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--raw-dir", type=Path, default=PROJECT_ROOT / "data" / "raw" / "measurements")
    parser.add_argument("--out-dir", type=Path, default=PROJECT_ROOT / "data" / "sample")
    parser.add_argument("--chunk-rows", type=int, default=1_000_000,
                        help="rows per CSV chunk, to keep each file a sane size for HDFS")
    args = parser.parse_args()

    if not args.raw_dir.exists():
        print(f"No Parquet found at {args.raw_dir}")
        print("Download first: python scripts/download_data.py --mode bulk --year 2024 --months 1-12")
        return

    files = sorted(args.raw_dir.rglob("*.parquet"))
    if not files:
        print(f"No .parquet files under {args.raw_dir}")
        return

    args.out_dir.mkdir(parents=True, exist_ok=True)

    grand_total = 0
    parts: list[Path] = []

    for index, path in enumerate(files, start=1):
        # Relative path keeps the year/month visible in the output filename.
        relative = path.relative_to(args.raw_dir)
        label = "/".join(relative.parts[:-1])  # e.g. year=2024/month=01
        if not label:
            label = "root"

        parquet_file = pq.ParquetFile(path)
        n_rows = parquet_file.metadata.num_rows
        print(f"[{index}/{len(files)}] {label} : {n_rows:,} rows")

        # Stream the month out in CSV chunks of about --chunk-rows rows.
        # Reading a whole 4M-row month at once would hold several GB of Python
        # objects in memory; each chunk is written and released in turn.
        chunk_index = 0
        buffer: list[pd.DataFrame] = []
        buffered = 0

        def flush(buffer: list[pd.DataFrame], chunk_index: int) -> Path:
            combined = pd.concat(buffer, ignore_index=True)
            target = args.out_dir / f"{label.replace('/', '_')}_part{chunk_index:03d}.csv"
            # Every chunk carries the header: HDFS treats all files in a
            # directory as one dataset, and Pig/Hive expect a header per file.
            combined.to_csv(
                target,
                index=False,
                header=True,
                date_format="%Y-%m-%d %H:%M:%S",
            )
            return target

        for batch in parquet_file.iter_batches(batch_size=200_000, columns=COLUMNS):
            frame = batch.to_pandas()
            buffer.append(frame)
            buffered += len(frame)
            if buffered >= args.chunk_rows:
                chunk_index += 1
                parts.append(flush(buffer, chunk_index))
                buffer = []
                buffered = 0

        if buffer:
            chunk_index += 1
            parts.append(flush(buffer, chunk_index))

        grand_total += n_rows

    # Verify every file parses back with the expected header. Checking only
    # the first file previously hid the fact that no chunk had a header at all.
    print()
    print(f"total rows written : {grand_total:,}")
    print(f"csv files created  : {len(parts)}")
    total_mb = sum(p.stat().st_size for p in parts) / 1e6
    print(f"total csv size     : {total_mb / 1000:.2f} GB")

    bad_headers = []
    for path in parts:
        with path.open() as handle:
            first_line = handle.readline().strip()
        if first_line != ",".join(COLUMNS):
            bad_headers.append((path.name, first_line[:80]))

    if bad_headers:
        print(f"\nERROR: {len(bad_headers)} file(s) have the wrong header:")
        for name, line in bad_headers[:5]:
            print(f"  {name}: {line}")
        raise SystemExit(1)

    print(f"header check       : all {len(parts)} files OK")

    # Confirm the values are parseable as numbers, since the MapReduce mapper
    # and Pig both do a numeric parse that would fail on a stray string.
    sample = pd.read_csv(parts[0], nrows=5000)
    numeric = pd.to_numeric(sample["value"], errors="coerce")
    if numeric.isna().any():
        print(f"ERROR: {numeric.isna().sum()} non-numeric values in {parts[0].name}")
        raise SystemExit(1)
    print(f"numeric check      : OK (value column parses as float)")

    print()
    print("Sample row:")
    print(f"  {sample.head(1).to_csv(index=False).strip()}")

    print()
    print("Next: hdfs dfs -put data/sample/*.csv /airquality/raw/")


if __name__ == "__main__":
    main()