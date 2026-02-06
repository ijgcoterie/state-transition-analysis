"""Cached data loading layer wrapping src/ pipeline."""

import sys
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

# Ensure project root is on path so src/ is importable
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.config import Config
from src.data_store import DataStore
from src.data_loader import load_sessions_data
from src.state_assignment import assign_states
from src.metrics import (
    calculate_state_distribution,
    calculate_transition_matrix,
    calculate_user_ever_transition_matrix,
    calculate_cohort_metrics,
    calculate_channel_metrics,
    calculate_time_to_state,
    build_sankey_data,
)

DEFAULT_DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "sessions.db")


@st.cache_data(ttl=3600, show_spinner=False)
def get_store_stats(db_path: str = DEFAULT_DB_PATH) -> dict:
    """Get database statistics for sidebar bounds. Cached 1 hour."""
    store = DataStore(store_path=db_path)
    return store.get_stats()


@st.cache_data(ttl=3600, show_spinner=False)
def get_first_touch_channels(db_path: str = DEFAULT_DB_PATH) -> list:
    """Get distinct first-session traffic source channels. Cached 1 hour."""
    store = DataStore(store_path=db_path)
    df = store.query(min_session_number=1, max_session_number=1)
    values = (
        df["SESSION_FIRST_TRAFFIC_SOURCE_CHANNEL_GROUPING"]
        .dropna()
        .unique()
        .tolist()
    )
    return sorted(values)


def filter_by_first_touch(df: pd.DataFrame, channels: tuple) -> pd.DataFrame:
    """Filter to users whose first session traffic source is in channels."""
    first_sessions = df[df["SESSION_NUMBER"] == 1]
    matching_users = first_sessions[
        first_sessions["SESSION_FIRST_TRAFFIC_SOURCE_CHANNEL_GROUPING"].isin(channels)
    ]["USER_ID"].unique()
    return df[df["USER_ID"].isin(matching_users)].copy()


@st.cache_data(ttl=600, show_spinner="Loading session data...")
def load_and_process(
    cohort_start: Optional[date],
    cohort_end: Optional[date],
    granularity: str,
    min_cohort_size: int,
    exploring_sources: tuple,
    db_path: str = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    """
    Load sessions from DataStore, assign states. Cached 10 minutes.

    All parameters must be hashable for Streamlit caching.
    """
    config = Config(
        cohort_start=cohort_start,
        cohort_end=cohort_end,
        cohort_granularity=granularity,
        min_cohort_size=min_cohort_size,
        exploring_traffic_sources=list(exploring_sources),
    )
    store = DataStore(store_path=db_path)
    df = load_sessions_data(store, config)
    df = assign_states(df, config)
    return df


@st.cache_data(ttl=600, show_spinner="Computing metrics...")
def compute_all_metrics(
    _df: pd.DataFrame,
    granularity: str,
    min_cohort_size: int,
    exploring_sources: tuple,
    max_sankey_sessions: int,
    cohort_start: Optional[date] = None,
    cohort_end: Optional[date] = None,
    first_touch_filter: tuple = (),
) -> dict:
    """
    Compute all metric sets from a processed DataFrame. Cached 10 minutes.

    _df is prefixed with _ so Streamlit skips hashing the DataFrame.
    Extra scalar params (including first_touch_filter) serve as cache keys.
    """
    config = Config(
        cohort_start=cohort_start,
        cohort_end=cohort_end,
        cohort_granularity=granularity,
        min_cohort_size=min_cohort_size,
        exploring_traffic_sources=list(exploring_sources),
        max_sessions_for_sankey=max_sankey_sessions,
    )

    state_dist = calculate_state_distribution(_df)
    transition_pct = calculate_transition_matrix(_df, normalize=True)
    transition_counts = calculate_transition_matrix(_df, normalize=False)
    user_transition_pct = calculate_user_ever_transition_matrix(_df, normalize=True)
    user_transition_counts = calculate_user_ever_transition_matrix(_df, normalize=False)
    cohort = calculate_cohort_metrics(_df, config) if "COHORT_PERIOD" in _df.columns else pd.DataFrame()
    channels = calculate_channel_metrics(_df, config)
    time_to_purchase = calculate_time_to_state(_df, target_state=4)
    sankey = build_sankey_data(_df, max_sessions=max_sankey_sessions)

    # Aggregate KPIs
    total_users = _df["USER_ID"].nunique()
    total_sessions = len(_df)
    purchasers = _df[_df["STATE"] == 4]["USER_ID"].nunique()
    returners = _df[_df["SESSION_NUMBER"] > 1]["USER_ID"].nunique()

    kpis = {
        "total_users": total_users,
        "total_sessions": total_sessions,
        "purchase_rate": (purchasers / total_users * 100) if total_users else 0,
        "return_rate": (returners / total_users * 100) if total_users else 0,
        "avg_sessions_per_user": (total_sessions / total_users) if total_users else 0,
    }

    return {
        "kpis": kpis,
        "state_dist": state_dist,
        "transition_pct": transition_pct,
        "transition_counts": transition_counts,
        "user_transition_pct": user_transition_pct,
        "user_transition_counts": user_transition_counts,
        "cohort": cohort,
        "channels": channels,
        "time_to_purchase": time_to_purchase,
        "sankey": sankey,
    }
