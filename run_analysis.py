#!/usr/bin/env python3
"""
CLI entry point for state transition analysis.

Usage:
    python run_analysis.py                           # Run with defaults
    python run_analysis.py --start 2026-01-01        # Specific cohort start
    python run_analysis.py --granularity M           # Monthly cohorts
    python run_analysis.py --data path/to/file.csv   # Specific data file

"""

import argparse
from datetime import datetime, date
from pathlib import Path
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from src.config import Config
from src.data_loader import load_sessions_data, validate_data
from src.state_assignment import assign_states, get_state_summary
from src.metrics import (
    calculate_state_distribution,
    calculate_transition_matrix,
    calculate_cohort_metrics,
    calculate_channel_metrics,
    build_sankey_data,
)
from src.export import export_all, export_for_dashboard


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    return datetime.strptime(date_str, '%Y-%m-%d').date()


def main():
    parser = argparse.ArgumentParser(description='Run state transition analysis')

    parser.add_argument(
        '--data', '-d',
        type=str,
        default='data/',
        help='Path to data file or directory (default: data/)'
    )
    parser.add_argument(
        '--start', '-s',
        type=str,
        default=None,
        help='Cohort start date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end', '-e',
        type=str,
        default=None,
        help='Cohort end date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--granularity', '-g',
        type=str,
        default='W',
        choices=['D', 'W', 'M'],
        help='Cohort granularity: D=daily, W=weekly, M=monthly (default: W)'
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='outputs/',
        help='Output directory (default: outputs/)'
    )
    parser.add_argument(
        '--min-cohort-size',
        type=int,
        default=50,
        help='Minimum cohort size for reporting (default: 50)'
    )
    parser.add_argument(
        '--dashboard',
        action='store_true',
        help='Also export dashboard-optimized JSON files'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Print detailed progress'
    )

    args = parser.parse_args()

    # Build config
    config = Config(
        data_path=args.data,
        cohort_start=parse_date(args.start) if args.start else None,
        cohort_end=parse_date(args.end) if args.end else None,
        cohort_granularity=args.granularity,
        min_cohort_size=args.min_cohort_size,
        output_path=args.output,
    )

    print("=" * 60)
    print("STATE TRANSITION ANALYSIS")
    print("=" * 60)

    # Load data
    print(f"\n1. Loading data from: {args.data}")
    df = load_sessions_data(args.data, config)

    # Validate
    print("\n2. Validating data...")
    validation = validate_data(df)
    if not validation['valid']:
        print(f"   ERROR: {validation['errors']}")
        sys.exit(1)

    print(f"   Sessions: {validation['summary']['total_sessions']:,}")
    print(f"   Users: {validation['summary']['unique_users']:,}")
    if validation['summary']['cohort_periods']:
        print(f"   Cohort periods: {validation['summary']['cohort_periods']}")

    if validation['warnings']:
        for warning in validation['warnings']:
            print(f"   WARNING: {warning}")

    # Assign states
    print("\n3. Assigning states...")
    df = assign_states(df, config)
    state_summary = get_state_summary(df)
    if args.verbose:
        print(state_summary.to_string(index=False))

    # Calculate metrics
    print("\n4. Calculating metrics...")

    results = {
        'state_entry_table': df[[
            'SESSION_ID', 'USER_ID', 'SESSION_NUMBER', 'SESSION_START', 'SESSION_END',
            'LANDING_PAGE', 'HAS_ADD_TO_CART', 'HAS_BEGIN_CHECKOUT', 'HAS_PURCHASE',
            'SESSION_FIRST_TRAFFIC_SOURCE_CHANNEL_GROUPING', 'STATE', 'STATE_NAME',
            'COHORT_PERIOD'
        ]],
        'state_distribution': calculate_state_distribution(df),
        'transition_matrix': calculate_transition_matrix(df),
        'cohort_metrics': calculate_cohort_metrics(df, config),
        'channel_breakdown': calculate_channel_metrics(df, config),
        'sankey_data': build_sankey_data(df, config.max_sessions_for_sankey),
        'summary': validation['summary'],
    }

    if args.verbose:
        print("\n   Transition Matrix (%):")
        print(results['transition_matrix'])

    # Export
    print(f"\n5. Exporting to: {args.output}")
    exported = export_all(results, config)
    for name, path in exported.items():
        print(f"   - {name}: {path}")

    if args.dashboard:
        print(f"\n6. Exporting dashboard files...")
        dashboard_exported = export_for_dashboard(results, f"{args.output}/dashboard/")
        for name, path in dashboard_exported.items():
            print(f"   - {name}: {path}")

    # Print key findings
    print("\n" + "=" * 60)
    print("KEY FINDINGS")
    print("=" * 60)

    total_users = df['USER_ID'].nunique()
    session1 = df[df['SESSION_NUMBER'] == 1]

    print(f"\nCohort: {total_users:,} users")
    print(f"Overall purchase rate: {df[df['STATE'] == 4]['USER_ID'].nunique() / total_users * 100:.1f}%")
    print(f"Return rate: {df[df['SESSION_NUMBER'] > 1]['USER_ID'].nunique() / total_users * 100:.1f}%")

    # Top channels by purchase-ready rate
    top_channels = results['channel_breakdown'][
        results['channel_breakdown']['total_users'] >= config.min_cohort_size
    ].nlargest(5, 'purchase_ready_rate')

    print(f"\nTop 5 channels by purchase-ready rate (min {config.min_cohort_size} users):")
    for _, row in top_channels.iterrows():
        print(f"   - {row['FIRST_TOUCH_CHANNEL']}: {row['purchase_ready_rate']:.1f}% ({int(row['total_users']):,} users)")

    print("\n" + "=" * 60)
    print("DONE")
    print("=" * 60)


if __name__ == '__main__':
    main()
