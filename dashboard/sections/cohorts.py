"""Cohort trends section: trend lines, cohort size bars, state mix area chart."""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.config import STATE_COLORS, STATE_NAMES


METRIC_OPTIONS = {
    "purchased_rate": "Purchase Rate (%)",
    "return_rate": "Return Rate (%)",
    "purchase_ready_rate": "Purchase-Ready Rate (%)",
    "problem_aware_rate": "Problem-Aware Rate (%)",
    "session1_purchase_rate": "Session 1 Purchase Rate (%)",
    "avg_sessions_per_user": "Avg Sessions / User",
}


def render(metrics: dict):
    """Render cohort trends tab."""
    cohort = metrics["cohort"]
    if cohort.empty:
        st.info("No cohort data available. Try adjusting date range or reducing minimum cohort size.")
        return

    st.caption(
        "A **cohort** is a group of users whose first-ever session fell within the same time period "
        "(controlled by the granularity filter). All metrics are **user-level**: e.g. a cohort's "
        "\"Purchase Rate\" is the % of users in that cohort who purchased at least once across all their sessions."
    )

    # Metric selector
    selected = st.multiselect(
        "Metrics to plot",
        options=list(METRIC_OPTIONS.keys()),
        default=["purchased_rate", "return_rate"],
        format_func=lambda k: METRIC_OPTIONS[k],
    )

    if selected:
        fig = go.Figure()
        for metric_key in selected:
            fig.add_trace(go.Scatter(
                x=cohort["COHORT_PERIOD"],
                y=cohort[metric_key],
                mode="lines+markers",
                name=METRIC_OPTIONS[metric_key],
            ))
        fig.update_layout(
            title="Cohort Metrics Over Time",
            xaxis_title="Cohort Period",
            yaxis_title="Value",
            hovermode="x unified",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Cohort size bars
    st.caption("Number of users in each cohort. Cohorts smaller than the minimum size threshold are excluded.")
    fig_size = px.bar(
        cohort,
        x="COHORT_PERIOD",
        y="total_users",
        labels={"total_users": "Users", "COHORT_PERIOD": "Cohort Period"},
        title="Cohort Sizes",
    )
    fig_size.update_layout(hovermode="x unified")
    st.plotly_chart(fig_size, use_container_width=True)

    # State mix area chart from cohort-level state distribution
    st.caption(
        "**State Reach Rates** show the % of users in each cohort who *ever* reached a given state. "
        "These are not mutually exclusive — a user who purchased also counts toward Problem-Aware and Purchase-Ready."
    )
    _render_state_mix_area(metrics)

    with st.expander("Cohort Data Table"):
        st.dataframe(cohort, use_container_width=True)


def _render_state_mix_area(metrics: dict):
    """Stacked area chart of state distribution over cohort periods."""
    cohort = metrics["cohort"]
    if cohort.empty:
        return

    state_cols = ["exploring_rate", "problem_aware_rate", "purchase_ready_rate", "purchased_rate"]
    available = [c for c in state_cols if c in cohort.columns]
    if not available:
        return

    rename = {
        "exploring_rate": "Exploring",
        "problem_aware_rate": "Problem-Aware",
        "purchase_ready_rate": "Purchase-Ready",
        "purchased_rate": "Purchased",
    }

    plot_df = cohort[["COHORT_PERIOD"] + available].copy()
    plot_df = plot_df.rename(columns=rename)
    melted = plot_df.melt(id_vars="COHORT_PERIOD", var_name="State", value_name="Rate (%)")

    color_map = {STATE_NAMES[k]: v for k, v in STATE_COLORS.items()}

    fig = px.area(
        melted,
        x="COHORT_PERIOD",
        y="Rate (%)",
        color="State",
        color_discrete_map=color_map,
        category_orders={"State": list(STATE_NAMES.values())},
        title="State Reach Rates Over Time",
        labels={"COHORT_PERIOD": "Cohort Period"},
    )
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)
