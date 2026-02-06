"""Channel performance section: bar chart and bubble scatter."""

import plotly.express as px
import streamlit as st


RANKING_METRICS = {
    "total_users": "Total Users",
    "purchase_rate": "Purchase Rate (%)",
    "purchase_ready_rate": "Purchase-Ready Rate (%)",
    "return_rate": "Return Rate (%)",
    "avg_sessions": "Avg Sessions / User",
    "session1_purchase_rate": "Session 1 Purchase Rate (%)",
}


def render(metrics: dict):
    """Render channel performance tab."""
    channels = metrics["channels"]
    if channels.empty:
        st.info("No channel data available.")
        return

    st.caption(
        "Channels are defined by the **first-touch traffic source** — the acquisition channel "
        "of each user's very first session. All metrics for a channel are computed across "
        "every session those users ever had, not just the first one."
    )

    # Ranking metric selector
    rank_by = st.selectbox(
        "Rank channels by",
        options=list(RANKING_METRICS.keys()),
        format_func=lambda k: RANKING_METRICS[k],
        index=1,
    )

    sorted_df = channels.sort_values(rank_by, ascending=True)

    fig_bar = px.bar(
        sorted_df,
        x=rank_by,
        y="FIRST_TOUCH_CHANNEL",
        orientation="h",
        labels={rank_by: RANKING_METRICS[rank_by], "FIRST_TOUCH_CHANNEL": "Channel"},
        title=f"Channels by {RANKING_METRICS[rank_by]}",
        text=rank_by,
    )
    fig_bar.update_traces(texttemplate="%{text:.1f}", textposition="outside")
    fig_bar.update_layout(height=max(350, len(sorted_df) * 35))
    st.plotly_chart(fig_bar, use_container_width=True)

    # Bubble scatter: purchase-ready rate vs purchase rate, sized by users
    st.caption(
        "The scatter plot below compares each channel's ability to generate purchase intent (x-axis) "
        "against its actual conversion rate (y-axis). Bubble size reflects user volume. "
        "Channels in the **top-right** are both effective at generating intent and converting. "
        "Large bubbles with high purchase-ready rate but low purchase rate may indicate "
        "friction in the checkout process for that audience."
    )
    if {"purchase_ready_rate", "purchase_rate", "total_users"}.issubset(channels.columns):
        fig_scatter = px.scatter(
            channels,
            x="purchase_ready_rate",
            y="purchase_rate",
            size="total_users",
            hover_name="FIRST_TOUCH_CHANNEL",
            labels={
                "purchase_ready_rate": "Purchase-Ready Rate (%)",
                "purchase_rate": "Purchase Rate (%)",
                "total_users": "Users",
            },
            title="Channel Efficiency: Purchase-Ready Rate vs Purchase Rate",
        )
        fig_scatter.update_layout(height=500)
        st.plotly_chart(fig_scatter, use_container_width=True)

    with st.expander("Channel Data Table"):
        st.dataframe(channels, use_container_width=True)
