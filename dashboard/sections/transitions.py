"""Transition matrix section: annotated heatmap with toggle."""

import plotly.figure_factory as ff
import streamlit as st


_DESCRIPTIONS = {
    "User-level (ever)": (
        "Of all users who were ever observed in a given state (row), what % eventually "
        "reached each other state (column) in a **later** session? This shows overall "
        "progression through the funnel regardless of how many sessions it took."
    ),
    "Session-level (next session)": (
        "Given a user is in state X during session N, what % of the time are they in "
        "state Y during session N+1? Each row sums to 100%. A single user with many "
        "sessions contributes multiple transitions."
    ),
}


def render(metrics: dict):
    """Render transition matrix tab."""
    view = st.radio(
        "Matrix type",
        options=["User-level (ever)", "Session-level (next session)"],
        horizontal=True,
    )
    st.caption(_DESCRIPTIONS[view])

    show_pct = st.toggle("Show percentages", value=True)

    if view == "User-level (ever)":
        matrix = metrics["user_transition_pct"] if show_pct else metrics["user_transition_counts"]
    else:
        matrix = metrics["transition_pct"] if show_pct else metrics["transition_counts"]

    if matrix.empty:
        st.info("No transition data available.")
        return

    z = matrix.values.tolist()
    x = list(matrix.columns)
    y = list(matrix.index)

    is_user_level = view == "User-level (ever)"
    fmt = ".1f" if show_pct else "d"
    suffix = "%" if show_pct else ""
    annotation_text = []
    for i, row in enumerate(z):
        ann_row = []
        for j, v in enumerate(row):
            if is_user_level and i == j:
                z[i][j] = None
                ann_row.append("")
            else:
                ann_row.append(f"{v:{fmt}}{suffix}")
        annotation_text.append(ann_row)

    fig = ff.create_annotated_heatmap(
        z=z,
        x=x,
        y=y,
        annotation_text=annotation_text,
        colorscale="Blues",
        showscale=True,
    )
    fig.update_layout(
        title="State Transition Matrix (row = from, col = to)",
        xaxis_title="To State",
        yaxis_title="From State",
        height=450,
        yaxis=dict(autorange="reversed"),
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Transition Data Table"):
        st.dataframe(matrix, use_container_width=True)
