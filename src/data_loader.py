"""Data loading and validation utilities."""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Union, TYPE_CHECKING
from datetime import date
import glob

from .config import Config, DEFAULT_CONFIG

if TYPE_CHECKING:
    from .data_store import DataStore


REQUIRED_COLUMNS = [
    'SESSION_ID',
    'SESSION_START',
    'SESSION_END',
    'SESSION_NUMBER',
    'USER_ID',
    'HAS_VIEW_ITEM',
    'HAS_ADD_TO_CART',
    'HAS_BEGIN_CHECKOUT',
    'HAS_PURCHASE',
    'LANDING_PAGE',
    'SESSION_FIRST_TRAFFIC_SOURCE_CHANNEL_GROUPING',
]


def load_sessions_data(
    source: Union[str, Path, pd.DataFrame, 'DataStore'],
    config: Config = DEFAULT_CONFIG,
) -> pd.DataFrame:
    """
    Load sessions data from file(s), DataFrame, or DataStore.

    Args:
        source: Path to CSV file, directory, existing DataFrame, or DataStore
        config: Configuration object

    Returns:
        Processed DataFrame with parsed dates and boolean columns
    """
    # Check if source is a DataStore
    if hasattr(source, 'get_cohort_sessions'):
        # It's a DataStore - use cohort query
        if config.cohort_start and config.cohort_end:
            df = source.get_cohort_sessions(
                str(config.cohort_start),
                str(config.cohort_end),
                include_future_sessions=True,
            )
            print(f"Loaded {len(df):,} sessions from data store (cohort query)")
        else:
            df = source.query()
            print(f"Loaded {len(df):,} sessions from data store (all data)")

        # DataStore already parses dates, but ensure cohort period is added
        df = _filter_to_cohort(df, config)
        return df

    if isinstance(source, pd.DataFrame):
        df = source.copy()
    elif Path(source).is_dir():
        # Check for SQLite database first
        db_path = Path(source) / 'sessions.db'
        if db_path.exists():
            from .data_store import DataStore
            store = DataStore(store_path=str(db_path))
            return load_sessions_data(store, config)

        # Fall back to loading CSVs
        pattern = str(Path(source) / config.file_pattern)
        files = glob.glob(pattern)
        if not files:
            raise FileNotFoundError(f"No files matching {pattern}")

        dfs = [pd.read_csv(f, engine='python') for f in files]
        df = pd.concat(dfs, ignore_index=True)
        print(f"Loaded {len(files)} files, {len(df):,} total rows")
    else:
        # Single file
        df = pd.read_csv(source, engine='python')

    # Parse and process
    df = _parse_dates(df)
    df = _parse_booleans(df)
    df = _filter_to_cohort(df, config)

    return df


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse date columns."""
    df = df.copy()
    df['SESSION_START'] = pd.to_datetime(df['SESSION_START'])
    df['SESSION_END'] = pd.to_datetime(df['SESSION_END'])
    df['SESSION_DATE'] = df['SESSION_START'].dt.date
    return df


def _parse_booleans(df: pd.DataFrame) -> pd.DataFrame:
    """Convert boolean columns from strings if needed."""
    df = df.copy()
    bool_cols = ['HAS_VIEW_ITEM', 'HAS_ADD_TO_CART', 'HAS_BEGIN_CHECKOUT', 'HAS_PURCHASE']

    for col in bool_cols:
        if col in df.columns and df[col].dtype == 'object':
            df[col] = df[col].str.lower() == 'true'

    return df


def _filter_to_cohort(
    df: pd.DataFrame,
    config: Config,
) -> pd.DataFrame:
    """Filter to users whose first session falls within cohort date range."""
    df = df.copy()

    # Find first session date for each user
    first_sessions = df[df['SESSION_NUMBER'] == 1].copy()
    first_sessions['FIRST_SESSION_DATE'] = first_sessions['SESSION_START'].dt.date

    # Apply cohort date filters
    mask = pd.Series(True, index=first_sessions.index)

    if config.cohort_start:
        mask &= first_sessions['FIRST_SESSION_DATE'] >= config.cohort_start
    if config.cohort_end:
        mask &= first_sessions['FIRST_SESSION_DATE'] <= config.cohort_end

    cohort_users = first_sessions[mask]['USER_ID'].unique()

    # Filter main dataframe to cohort users
    df = df[df['USER_ID'].isin(cohort_users)].copy()

    # Add cohort period for each user
    user_cohort = first_sessions[['USER_ID', 'FIRST_SESSION_DATE']].drop_duplicates()
    user_cohort['FIRST_SESSION_DATE'] = pd.to_datetime(user_cohort['FIRST_SESSION_DATE'])
    user_cohort['COHORT_PERIOD'] = user_cohort['FIRST_SESSION_DATE'].dt.to_period(config.cohort_granularity)

    df = df.merge(user_cohort[['USER_ID', 'COHORT_PERIOD']], on='USER_ID', how='left')

    return df


def validate_data(df: pd.DataFrame) -> dict:
    """
    Validate data quality and return summary.

    Returns:
        Dictionary with validation results
    """
    results = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'summary': {}
    }

    # Check required columns
    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_cols:
        results['valid'] = False
        results['errors'].append(f"Missing required columns: {missing_cols}")

    # Check for nulls in key columns
    null_counts = df[['SESSION_ID', 'USER_ID', 'SESSION_NUMBER']].isnull().sum()
    if null_counts.any():
        results['valid'] = False
        results['errors'].append(f"Null values in key columns: {null_counts[null_counts > 0].to_dict()}")

    # Check session number sequence
    user_session_gaps = df.groupby('USER_ID')['SESSION_NUMBER'].apply(
        lambda x: (x.sort_values().diff().dropna() != 1).any()
    )
    if user_session_gaps.any():
        gap_users = user_session_gaps[user_session_gaps].index.tolist()[:5]
        results['warnings'].append(f"Session number gaps detected for {len(gap_users)} users (e.g., {gap_users})")

    # Summary stats
    results['summary'] = {
        'total_sessions': len(df),
        'unique_users': df['USER_ID'].nunique(),
        'date_range': (df['SESSION_START'].min(), df['SESSION_START'].max()),
        'cohort_periods': df['COHORT_PERIOD'].nunique() if 'COHORT_PERIOD' in df.columns else None,
    }

    return results
