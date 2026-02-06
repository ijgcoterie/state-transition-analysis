"""Sankey flow diagram section."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.config import STATE_COLORS


# Colors for sankey nodes
_NODE_COLORS = {
    1: STATE_COLORS[1],
    2: STATE_COLORS[2],
    3: STATE_COLORS[3],
    4: STATE_COLORS[4],
    "churned": "#e74c3c",
}


def render(metrics: dict):
    """Render Sankey flow diagram tab."""
    sankey_data = metrics["sankey"]
    nodes = sankey_data["nodes"]
    links = sankey_data["links"]

    st.caption(
        "Each column represents a session number (1st session, 2nd session, etc.) and each node is a "
        "state within that session. The width of a flow between two nodes is proportional to the number "
        "of users who made that transition. **Churned** nodes capture users who did not return for "
        "another session. Adjust the \"Max Sankey sessions\" slider in the sidebar to show more or fewer sessions."
    )

    if not links:
        st.info("No flow data available. Users may not have enough sessions.")
        return

    # Build node colors
    node_colors = []
    for node in nodes:
        state = node["state"]
        node_colors.append(_NODE_COLORS.get(state, "#95a5a6"))

    # Build link colors (lighter version of source node color)
    link_colors = []
    for link in links:
        src_state = nodes[link["source"]]["state"]
        base = _NODE_COLORS.get(src_state, "#95a5a6")
        link_colors.append(_hex_with_opacity(base, 0.4))

    fig = go.Figure(go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=[n["name"] for n in nodes],
            color=node_colors,
        ),
        link=dict(
            source=[l["source"] for l in links],
            target=[l["target"] for l in links],
            value=[l["value"] for l in links],
            color=link_colors,
        ),
    ))
    fig.update_layout(
        title="Session State Flow",
        height=600,
        font=dict(size=16, family="Arial, sans-serif"),
        margin=dict(l=10, r=10, t=50, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Churn summary table
    churn_nodes = [n for n in nodes if n["state"] == "churned"]
    if churn_nodes:
        churn_data = []
        for cn in churn_nodes:
            inbound = sum(l["value"] for l in links if l["target"] == cn["id"])
            if inbound > 0:
                churn_data.append({"Stage": cn["name"], "Users Churned": inbound})
        if churn_data:
            st.subheader("Churn Summary")
            st.dataframe(pd.DataFrame(churn_data), use_container_width=True, hide_index=True)

    with st.expander("Sankey Data (Nodes & Links)"):
        st.json(sankey_data)


def _hex_with_opacity(hex_color: str, opacity: float) -> str:
    """Convert hex color to rgba string with given opacity."""
    hex_color = hex_color.lstrip("#")
    r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)
    return f"rgba({r},{g},{b},{opacity})"
