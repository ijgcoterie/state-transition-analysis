"""Vectorized state assignment logic."""

import pandas as pd
import numpy as np
from typing import List

from .config import STATE_NAMES, Config, DEFAULT_CONFIG


def assign_states(
    df: pd.DataFrame,
    config: Config = DEFAULT_CONFIG,
) -> pd.DataFrame:
    """
    Assign states to each session using vectorized operations.

    States are hierarchical (4 > 3 > 2 > 1):
        4: Purchased - HAS_PURCHASE = True
        3: Purchase-Ready - HAS_ADD_TO_CART or HAS_BEGIN_CHECKOUT (no purchase)
        2: Problem-Aware - Returning visitor OR non-Facebook first session
        1: Exploring - First session from Facebook (default)

    Args:
        df: Sessions DataFrame
        config: Configuration object

    Returns:
        DataFrame with STATE and STATE_NAME columns added
    """
    df = df.copy()

    # Build traffic source pattern check
    traffic_col = 'SESSION_FIRST_TRAFFIC_SOURCE_CHANNEL_GROUPING'
    is_exploring_source = _check_traffic_sources(
        df[traffic_col],
        config.exploring_traffic_sources
    )

    # Vectorized conditions (checked in order of hierarchy, highest first)
    conditions = [
        # State 4: Purchased
        df['HAS_PURCHASE'].fillna(False),

        # State 3: Purchase-Ready (add to cart or begin checkout, but no purchase)
        (df['HAS_ADD_TO_CART'].fillna(False) | df['HAS_BEGIN_CHECKOUT'].fillna(False)),

        # State 2: Problem-Aware
        # - NOT first session (session > 1), OR
        # - IS first session AND not from exploring sources (e.g., Facebook)
        (df['SESSION_NUMBER'] > 1) | ((df['SESSION_NUMBER'] == 1) & ~is_exploring_source),
    ]

    choices = [4, 3, 2]

    # Default is State 1 (Exploring)
    df['STATE'] = np.select(conditions, choices, default=1)
    df['STATE_NAME'] = df['STATE'].map(STATE_NAMES)

    return df


def _check_traffic_sources(
    traffic_series: pd.Series,
    source_patterns: List[str],
) -> pd.Series:
    """
    Check if traffic source matches any of the given patterns.

    Args:
        traffic_series: Series of traffic source strings
        source_patterns: List of patterns to match (prefix match)

    Returns:
        Boolean series indicating matches
    """
    traffic_filled = traffic_series.fillna('')

    if not source_patterns:
        return pd.Series(False, index=traffic_series.index)

    # Check if traffic source starts with any of the patterns
    mask = pd.Series(False, index=traffic_series.index)
    for pattern in source_patterns:
        mask |= traffic_filled.str.startswith(pattern, na=False)

    return mask


def get_state_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get summary of state distribution.

    Args:
        df: DataFrame with STATE column

    Returns:
        Summary DataFrame with counts and percentages
    """
    summary = df.groupby(['STATE', 'STATE_NAME']).size().reset_index(name='sessions')
    summary['pct'] = (summary['sessions'] / len(df) * 100).round(1)
    return summary.sort_values('STATE')
