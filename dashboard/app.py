"""
State Transition Analysis Dashboard

Entry point: streamlit run dashboard/app.py
"""

import sys
from datetime import date, datetime
from pathlib import Path

import streamlit as st

# Ensure project root is importable
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from dashboard.data import get_store_stats, get_first_touch_channels, filter_by_first_touch, load_and_process, compute_all_metrics
from dashboard.sections import overview, cohorts, transitions, channels, sankey, time_to_state
from src.data_store import DataStore

# -- Page config --
st.set_page_config(
    page_title="State Transition Analysis",
    page_icon=":bar_chart:",
    layout="wide",
)

st.title("State Transition Analysis")

# -- Sidebar --
with st.sidebar:
    st.header("Filters")

    # Load DB stats to set default bounds
    try:
        stats = get_store_stats()
    except Exception as e:
        st.error(f"Could not connect to data store: {e}")
        st.stop()

    date_range = stats.get("date_range", {})
    min_date_str = date_range.get("min_date")
    max_date_str = date_range.get("max_date")

    if min_date_str and max_date_str:
        data_min = datetime.strptime(str(min_date_str), "%Y-%m-%d").date()
        data_max = datetime.strptime(str(max_date_str), "%Y-%m-%d").date()
    else:
        data_min = date(2023, 1, 1)
        data_max = date.today()

    cohort_start, cohort_end = st.date_input(
        "Cohort date range",
        value=(data_min, data_max),
        min_value=data_min,
        max_value=data_max,
    )

    granularity_label = st.selectbox(
        "Cohort granularity",
        options=["Daily", "Weekly", "Monthly"],
        index=1,
    )
    granularity_map = {"Daily": "D", "Weekly": "W", "Monthly": "M"}
    granularity = granularity_map[granularity_label]

    min_cohort_size = st.slider("Min cohort size", min_value=10, max_value=500, value=50, step=10)

    exploring_sources = st.multiselect(
        "Exploring traffic sources",
        options=["Facebook", "Facebook Ads", "Facebook / Organic"],
        default=["Facebook"],
    )

    max_sankey_sessions = st.slider("Max Sankey sessions", min_value=2, max_value=6, value=3)

    # First-touch channel filter
    all_channels = get_first_touch_channels()
    first_touch_filter = st.multiselect(
        "First-touch channel (user's first session)",
        options=all_channels,
        default=[],
        help="Filter to users whose very first session came from these channels. Leave empty for all.",
    )

    st.caption(
        f"**{stats.get('total_sessions', 0):,}** sessions | "
        f"**{stats.get('total_users', 0):,}** users | "
        f"{min_date_str} to {max_date_str}"
    )

    # Ingest CSVs from data/ folder
    st.divider()
    if st.button("Ingest CSVs from data/ folder"):
        store = DataStore(store_path=str(Path(_project_root) / "data" / "sessions.db"))
        results = store.ingest_directory(str(Path(_project_root) / "data"))
        new = sum(r.get("new_rows", 0) for r in results)
        skipped = sum(1 for r in results if r.get("status") == "skipped")
        if new > 0:
            st.success(f"Ingested **{new:,}** new rows from {len(results) - skipped} file(s).")
        else:
            st.info("No new data found. All files already imported.")
        # Clear caches so new data is picked up
        get_store_stats.clear()
        get_first_touch_channels.clear()
        load_and_process.clear()
        compute_all_metrics.clear()
        st.rerun()

# -- Validate date input (Streamlit returns a tuple that may be incomplete) --
if not isinstance(cohort_start, date) or not isinstance(cohort_end, date):
    st.warning("Please select both a start and end date.")
    st.stop()

# -- Load & compute --
try:
    df = load_and_process(
        cohort_start=cohort_start,
        cohort_end=cohort_end,
        granularity=granularity,
        min_cohort_size=min_cohort_size,
        exploring_sources=tuple(sorted(exploring_sources)),
    )
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()

if df.empty:
    st.warning("No data found for the selected filters. Try widening the date range.")
    st.stop()

# Apply first-touch channel filter
first_touch_tuple = tuple(sorted(first_touch_filter))
if first_touch_tuple:
    df = filter_by_first_touch(df, first_touch_tuple)
    if df.empty:
        st.warning("No users match the selected first-touch channels. Try different channels.")
        st.stop()

metrics = compute_all_metrics(
    _df=df,
    granularity=granularity,
    min_cohort_size=min_cohort_size,
    exploring_sources=tuple(sorted(exploring_sources)),
    max_sankey_sessions=max_sankey_sessions,
    cohort_start=cohort_start,
    cohort_end=cohort_end,
    first_touch_filter=first_touch_tuple,
)

# -- Overview strip (always visible) --
overview.render(metrics)

st.divider()

# -- Tabs --
tab_cohort, tab_transition, tab_channel, tab_sankey, tab_ttp = st.tabs([
    "Cohort Trends",
    "Transition Matrix",
    "Channel Performance",
    "Session Flow",
    "Time to Purchase",
])

with tab_cohort:
    cohorts.render(metrics)

with tab_transition:
    transitions.render(metrics)

with tab_channel:
    channels.render(metrics)

with tab_sankey:
    sankey.render(metrics)

with tab_ttp:
    time_to_state.render(metrics)
