"""Time-to-purchase section: histograms and KPI cards."""

import plotly.express as px
import streamlit as st


def render(metrics: dict):
    """Render time-to-purchase tab."""
    ttp = metrics["time_to_purchase"]

    st.caption(
        "Measures how long it takes users to make their first purchase, in both session count and "
        "calendar days. Only users who actually purchased are included. \"Sessions to Purchase\" = the "
        "session number in which the user first purchased (1 = bought on first visit). "
        "\"Days to Purchase\" = calendar days between a user's first-ever session and their first purchase session."
    )

    if ttp.empty:
        st.info("No users have reached the Purchased state yet.")
        return

    # KPI cards
    cols = st.columns(3)
    cols[0].metric("Purchasers", f"{len(ttp):,}")
    cols[1].metric("Median Sessions to Purchase", f"{ttp['sessions_to_state'].median():.0f}")
    cols[2].metric("Median Days to Purchase", f"{ttp['days_to_state'].median():.1f}")

    # Sessions histogram
    fig_sessions = px.histogram(
        ttp,
        x="sessions_to_state",
        nbins=min(30, int(ttp["sessions_to_state"].max())),
        labels={"sessions_to_state": "Sessions to Purchase"},
        title="Distribution: Sessions to First Purchase",
    )
    fig_sessions.update_layout(bargap=0.1)
    st.plotly_chart(fig_sessions, use_container_width=True)

    # Days histogram (bucket into reasonable bins)
    fig_days = px.histogram(
        ttp,
        x="days_to_state",
        nbins=30,
        labels={"days_to_state": "Days to Purchase"},
        title="Distribution: Days to First Purchase",
    )
    fig_days.update_layout(bargap=0.1)
    st.plotly_chart(fig_days, use_container_width=True)

    with st.expander("Time-to-Purchase Data Table"):
        display_df = ttp[["USER_ID", "sessions_to_state", "days_to_state"]].copy()
        display_df.columns = ["User ID", "Sessions", "Days"]
        st.dataframe(display_df, use_container_width=True)
