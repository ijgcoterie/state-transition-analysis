"""Overview section: KPI cards and state distribution bar."""

import plotly.express as px
import streamlit as st

from src.config import STATE_COLORS, STATE_NAMES


def render(metrics: dict):
    """Render the overview strip with KPIs and state distribution."""
    kpis = metrics["kpis"]

    # KPI cards
    cols = st.columns(5)
    cols[0].metric("Users", f"{kpis['total_users']:,}")
    cols[1].metric("Sessions", f"{kpis['total_sessions']:,}")
    cols[2].metric("Purchase Rate", f"{kpis['purchase_rate']:.1f}%")
    cols[3].metric("Return Rate", f"{kpis['return_rate']:.1f}%")
    cols[4].metric("Avg Sessions/User", f"{kpis['avg_sessions_per_user']:.1f}")
    st.caption(
        "**Users** = unique users in the filtered cohort. "
        "**Purchase Rate** = % of users who purchased at least once. "
        "**Return Rate** = % of users who came back for a second session."
    )

    # Horizontal stacked bar of state distribution
    dist = metrics["state_dist"].copy()
    if dist.empty:
        return

    color_map = {STATE_NAMES[k]: v for k, v in STATE_COLORS.items()}

    fig = px.bar(
        dist,
        x="pct",
        y=["All Sessions"] * len(dist),
        color="STATE_NAME",
        orientation="h",
        color_discrete_map=color_map,
        category_orders={"STATE_NAME": list(STATE_NAMES.values())},
        labels={"pct": "% of Sessions", "y": ""},
        text="pct",
    )
    fig.update_traces(texttemplate="%{text:.1f}%", textposition="inside")
    fig.update_layout(
        height=100,
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        barmode="stack",
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption(
        "Each session is assigned exactly one state based on the highest-level action observed: "
        "**Purchased** (completed purchase) > **Purchase-Ready** (added to cart or began checkout) > "
        "**Problem-Aware** (returning visitor or non-exploring-source first visit) > "
        "**Exploring** (first session from an exploring traffic source). "
        "The bar shows what share of all sessions falls into each state."
    )
