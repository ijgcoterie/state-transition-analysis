#!/usr/bin/env python3
"""
Ingest new CSV files into the data store.

Usage:
    python ingest_data.py                           # Ingest all CSVs in data/
    python ingest_data.py path/to/file.csv          # Ingest specific file
    python ingest_data.py --backend parquet         # Use Parquet instead of SQLite

This script handles:
- Deduplication (same SESSION_ID won't be added twice)
- Tracking which files have been imported
- Consolidating data across multiple time periods
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from src.data_store import DataStore


def main():
    parser = argparse.ArgumentParser(description='Ingest CSV files into data store')

    parser.add_argument(
        'source',
        nargs='?',
        default='data/',
        help='CSV file or directory to ingest (default: data/)'
    )
    parser.add_argument(
        '--backend', '-b',
        choices=['sqlite', 'parquet'],
        default='sqlite',
        help='Storage backend (default: sqlite)'
    )
    parser.add_argument(
        '--store', '-s',
        default='data/sessions.db',
        help='Path to data store (default: data/sessions.db)'
    )
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Re-import files even if already imported'
    )
    parser.add_argument(
        '--pattern', '-p',
        default='*_SESSIONS_COHORT.csv',
        help='Glob pattern for CSV files (default: *_SESSIONS_COHORT.csv)'
    )

    args = parser.parse_args()

    # Adjust store path for parquet
    store_path = args.store
    if args.backend == 'parquet' and store_path.endswith('.db'):
        store_path = store_path.replace('.db', '.parquet')

    print("=" * 60)
    print("DATA INGESTION")
    print("=" * 60)
    print(f"Backend: {args.backend}")
    print(f"Store: {store_path}")
    print(f"Source: {args.source}")
    print()

    # Initialize data store
    store = DataStore(store_path=store_path, backend=args.backend)

    # Ingest
    source = Path(args.source)
    if source.is_dir():
        print(f"Scanning for files matching: {args.pattern}")
        results = store.ingest_directory(
            directory=str(source),
            pattern=args.pattern,
            skip_if_imported=not args.force,
        )
    else:
        results = [store.ingest_csv(source, skip_if_imported=not args.force)]
        for r in results:
            print(f"  {source.name}: {r.get('status')} ({r.get('new_rows', r.get('reason', '?'))})")

    # Summary
    print()
    print("-" * 60)

    total_new = sum(r.get('new_rows', 0) for r in results)
    total_skipped = sum(1 for r in results if r.get('status') == 'skipped')
    total_dupes = sum(r.get('duplicate_rows', 0) for r in results)

    print(f"Files processed: {len(results)}")
    print(f"Files skipped (already imported): {total_skipped}")
    print(f"New sessions added: {total_new:,}")
    print(f"Duplicate sessions ignored: {total_dupes:,}")

    # Show store stats
    print()
    print("Data store statistics:")
    stats = store.get_stats()
    print(f"  Total sessions: {stats['total_sessions']:,}")
    print(f"  Total users: {stats['total_users']:,}")
    print(f"  Date range: {stats['date_range']['min_date']} to {stats['date_range']['max_date']}")

    print()
    print("=" * 60)
    print("DONE")
    print("=" * 60)


if __name__ == '__main__':
    main()
