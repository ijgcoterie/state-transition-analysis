"""Metrics calculation functions."""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any

from .config import STATE_NAMES, Config, DEFAULT_CONFIG


def calculate_state_distribution(
    df: pd.DataFrame,
    group_by: Optional[str] = None,
) -> pd.DataFrame:
    """
    Calculate state distribution, optionally grouped.

    Args:
        df: DataFrame with STATE column
        group_by: Optional column to group by (e.g., 'COHORT_PERIOD')

    Returns:
        State distribution DataFrame
    """
    if group_by and group_by in df.columns:
        dist = df.groupby([group_by, 'STATE', 'STATE_NAME']).size().reset_index(name='sessions')
        totals = df.groupby(group_by).size().reset_index(name='total')
        dist = dist.merge(totals, on=group_by)
        dist['pct'] = (dist['sessions'] / dist['total'] * 100).round(1)
        dist = dist.drop(columns=['total'])
    else:
        dist = df.groupby(['STATE', 'STATE_NAME']).size().reset_index(name='sessions')
        dist['pct'] = (dist['sessions'] / len(df) * 100).round(1)

    return dist.sort_values(['STATE'] if not group_by else [group_by, 'STATE'])


def calculate_transition_matrix(
    df: pd.DataFrame,
    normalize: bool = True,
) -> pd.DataFrame:
    """
    Calculate session-to-session state transition matrix.

    Args:
        df: DataFrame with STATE column, sorted by user and session
        normalize: If True, return percentages; if False, return counts

    Returns:
        Transition matrix DataFrame
    """
    df_sorted = df.sort_values(['USER_ID', 'SESSION_NUMBER']).copy()
    df_sorted['PREV_STATE'] = df_sorted.groupby('USER_ID')['STATE'].shift(1)

    # Filter to transitions only (exclude first sessions)
    transitions = df_sorted[df_sorted['PREV_STATE'].notna()].copy()
    transitions['PREV_STATE'] = transitions['PREV_STATE'].astype(int)

    # Build matrix
    matrix = pd.crosstab(
        transitions['PREV_STATE'].map(STATE_NAMES),
        transitions['STATE'].map(STATE_NAMES),
        normalize='index' if normalize else False,
    )

    if normalize:
        matrix = matrix * 100

    # Reorder
    state_order = list(STATE_NAMES.values())
    matrix = matrix.reindex(index=state_order, columns=state_order, fill_value=0)

    return matrix.round(1) if normalize else matrix


def calculate_cohort_metrics(
    df: pd.DataFrame,
    config: Config = DEFAULT_CONFIG,
) -> pd.DataFrame:
    """
    Calculate metrics by cohort period.

    Args:
        df: DataFrame with STATE and COHORT_PERIOD columns
        config: Configuration object

    Returns:
        Cohort metrics DataFrame
    """
    if 'COHORT_PERIOD' not in df.columns:
        raise ValueError("COHORT_PERIOD column required. Run data_loader with cohort config.")

    def calc_metrics(group):
        users = group['USER_ID'].unique()
        total_users = len(users)

        if total_users < config.min_cohort_size:
            return None

        session1 = group[group['SESSION_NUMBER'] == 1]

        return pd.Series({
            'total_users': total_users,
            'total_sessions': len(group),
            'avg_sessions_per_user': len(group) / total_users,

            # State reach rates (ever)
            'exploring_rate': group[group['STATE'] == 1]['USER_ID'].nunique() / total_users * 100,
            'problem_aware_rate': group[group['STATE'] >= 2]['USER_ID'].nunique() / total_users * 100,
            'purchase_ready_rate': group[group['STATE'] == 3]['USER_ID'].nunique() / total_users * 100,
            'purchased_rate': group[group['STATE'] == 4]['USER_ID'].nunique() / total_users * 100,

            # Session 1 rates
            'session1_purchase_rate': len(session1[session1['STATE'] == 4]) / total_users * 100,

            # Return rate
            'return_rate': len(group[group['SESSION_NUMBER'] > 1]['USER_ID'].unique()) / total_users * 100,
        })

    metrics = df.groupby('COHORT_PERIOD', group_keys=False).apply(calc_metrics)
    metrics = metrics.dropna().reset_index()

    # Convert period to string for easier handling
    metrics['COHORT_PERIOD'] = metrics['COHORT_PERIOD'].astype(str)

    return metrics.round(2)


def calculate_channel_metrics(
    df: pd.DataFrame,
    config: Config = DEFAULT_CONFIG,
) -> pd.DataFrame:
    """
    Calculate metrics by first-touch acquisition channel.

    Args:
        df: DataFrame with STATE column
        config: Configuration object

    Returns:
        Channel metrics DataFrame
    """
    # Get first-touch channel for each user
    first_sessions = df[df['SESSION_NUMBER'] == 1][['USER_ID', 'SESSION_FIRST_TRAFFIC_SOURCE_CHANNEL_GROUPING']].copy()
    first_sessions.columns = ['USER_ID', 'FIRST_TOUCH_CHANNEL']

    df_with_channel = df.merge(first_sessions, on='USER_ID', how='left')

    def calc_metrics(group):
        users = group['USER_ID'].unique()
        total_users = len(users)

        session1 = group[group['SESSION_NUMBER'] == 1]

        return pd.Series({
            'total_users': total_users,
            'purchased_users': group[group['STATE'] == 4]['USER_ID'].nunique(),
            'purchase_rate': group[group['STATE'] == 4]['USER_ID'].nunique() / total_users * 100,
            'purchase_ready_users': group[group['STATE'] == 3]['USER_ID'].nunique(),
            'purchase_ready_rate': group[group['STATE'] == 3]['USER_ID'].nunique() / total_users * 100,
            'problem_aware_rate': group[group['STATE'] >= 2]['USER_ID'].nunique() / total_users * 100,
            'avg_sessions': len(group) / total_users,
            'session1_purchase_rate': len(session1[session1['STATE'] == 4]) / total_users * 100,
            'return_rate': group[group['SESSION_NUMBER'] > 1]['USER_ID'].nunique() / total_users * 100,
        })

    metrics = df_with_channel.groupby('FIRST_TOUCH_CHANNEL', group_keys=False).apply(calc_metrics)
    metrics = metrics.reset_index()
    metrics = metrics.sort_values('total_users', ascending=False)

    return metrics.round(2)


def calculate_time_to_state(
    df: pd.DataFrame,
    target_state: int = 4,
) -> pd.DataFrame:
    """
    Calculate time/sessions to reach a target state.

    Args:
        df: DataFrame with STATE column
        target_state: State to measure time to

    Returns:
        DataFrame with time-to-state metrics per user
    """
    df_sorted = df.sort_values(['USER_ID', 'SESSION_NUMBER']).copy()

    # Find first session where user reached target state
    reached = df_sorted[df_sorted['STATE'] >= target_state].groupby('USER_ID').agg({
        'SESSION_NUMBER': 'min',
        'SESSION_START': 'min',
    }).reset_index()

    reached.columns = ['USER_ID', 'sessions_to_state', 'first_reached_at']

    # Get user's first session time
    first_session = df_sorted[df_sorted['SESSION_NUMBER'] == 1][['USER_ID', 'SESSION_START']]
    first_session.columns = ['USER_ID', 'first_session_at']

    reached = reached.merge(first_session, on='USER_ID')
    reached['days_to_state'] = (reached['first_reached_at'] - reached['first_session_at']).dt.total_seconds() / 86400

    return reached


def build_sankey_data(
    df: pd.DataFrame,
    max_sessions: int = 3,
) -> Dict[str, Any]:
    """
    Build Sankey diagram data for state flow visualization.

    Args:
        df: DataFrame with STATE column
        max_sessions: Maximum number of sessions to include

    Returns:
        Dictionary with 'nodes' and 'links' for Sankey diagram
    """
    nodes = []
    links = []
    node_map = {}

    # Create nodes for each session-state combination
    for session_num in range(1, max_sessions + 1):
        for state_num, state_name in STATE_NAMES.items():
            node_id = len(nodes)
            node_map[(session_num, state_num)] = node_id
            nodes.append({
                'id': node_id,
                'name': f"S{session_num}: {state_name}",
                'session': session_num,
                'state': state_num,
                'state_name': state_name
            })

    # Add "Churned" nodes
    for session_num in range(1, max_sessions):
        node_id = len(nodes)
        node_map[(session_num, 'churned')] = node_id
        nodes.append({
            'id': node_id,
            'name': f"After S{session_num}: Churned",
            'session': session_num,
            'state': 'churned',
            'state_name': 'Churned'
        })

    # Calculate user sets by session and state
    users_by_session_state = {}
    for session_num in range(1, max_sessions + 1):
        session_data = df[df['SESSION_NUMBER'] == session_num]
        for state_num in STATE_NAMES.keys():
            users = set(session_data[session_data['STATE'] == state_num]['USER_ID'])
            users_by_session_state[(session_num, state_num)] = users

    # Build links
    for session_num in range(1, max_sessions):
        next_session = session_num + 1
        next_session_users = set(df[df['SESSION_NUMBER'] == next_session]['USER_ID'])

        for from_state in STATE_NAMES.keys():
            from_users = users_by_session_state.get((session_num, from_state), set())

            for to_state in STATE_NAMES.keys():
                to_users = users_by_session_state.get((next_session, to_state), set())
                flow_users = from_users & to_users

                if flow_users:
                    links.append({
                        'source': node_map[(session_num, from_state)],
                        'target': node_map[(next_session, to_state)],
                        'value': len(flow_users)
                    })

            # Churned users
            churned_users = from_users - next_session_users
            if churned_users:
                links.append({
                    'source': node_map[(session_num, from_state)],
                    'target': node_map[(session_num, 'churned')],
                    'value': len(churned_users)
                })

    return {'nodes': nodes, 'links': links}
