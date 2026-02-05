"""Export utilities for analysis outputs."""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

from .config import Config, DEFAULT_CONFIG


def export_all(
    results: Dict[str, Any],
    config: Config = DEFAULT_CONFIG,
    run_id: Optional[str] = None,
) -> Dict[str, str]:
    """
    Export all analysis results to configured formats.

    Args:
        results: Dictionary containing analysis results
            Expected keys: state_entry_table, transition_metrics,
                          cohort_metrics, channel_breakdown, sankey_data
        config: Configuration object
        run_id: Optional identifier for this run (defaults to timestamp)

    Returns:
        Dictionary mapping result names to output file paths
    """
    output_dir = Path(config.output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    if run_id is None:
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')

    exported = {}

    # Export DataFrames
    df_exports = [
        'state_entry_table',
        'transition_metrics',
        'cohort_metrics',
        'channel_breakdown',
        'state_distribution',
    ]

    for name in df_exports:
        if name in results and results[name] is not None:
            df = results[name]

            if 'csv' in config.export_formats:
                path = output_dir / f"{name}.csv"
                df.to_csv(path, index=False)
                exported[name] = str(path)

    # Export Sankey data as JSON
    if 'sankey_data' in results and results['sankey_data'] is not None:
        if 'json' in config.export_formats:
            path = output_dir / "sankey_data.json"
            with open(path, 'w') as f:
                json.dump(results['sankey_data'], f, indent=2)
            exported['sankey_data'] = str(path)

    # Export transition matrix
    if 'transition_matrix' in results and results['transition_matrix'] is not None:
        if 'csv' in config.export_formats:
            path = output_dir / "transition_matrix.csv"
            results['transition_matrix'].to_csv(path)
            exported['transition_matrix'] = str(path)

    # Export summary/metadata
    if 'summary' in results:
        path = output_dir / "run_summary.json"
        summary = results['summary'].copy()
        summary['run_id'] = run_id
        summary['exported_at'] = datetime.now().isoformat()

        # Convert any non-serializable types
        for k, v in summary.items():
            if isinstance(v, pd.Timestamp):
                summary[k] = v.isoformat()
            elif hasattr(v, 'tolist'):
                summary[k] = v.tolist()

        with open(path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        exported['summary'] = str(path)

    return exported


def export_for_dashboard(
    results: Dict[str, Any],
    output_path: str = 'outputs/dashboard/',
) -> Dict[str, str]:
    """
    Export data in format optimized for dashboard consumption.

    Creates JSON files suitable for web dashboards.

    Args:
        results: Dictionary containing analysis results
        output_path: Output directory path

    Returns:
        Dictionary mapping result names to output file paths
    """
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    exported = {}

    # Cohort metrics as JSON (for time series charts)
    if 'cohort_metrics' in results and results['cohort_metrics'] is not None:
        path = output_dir / "cohort_metrics.json"
        results['cohort_metrics'].to_json(path, orient='records', indent=2)
        exported['cohort_metrics'] = str(path)

    # Channel breakdown as JSON
    if 'channel_breakdown' in results and results['channel_breakdown'] is not None:
        path = output_dir / "channel_breakdown.json"
        results['channel_breakdown'].to_json(path, orient='records', indent=2)
        exported['channel_breakdown'] = str(path)

    # State distribution as JSON
    if 'state_distribution' in results and results['state_distribution'] is not None:
        path = output_dir / "state_distribution.json"
        results['state_distribution'].to_json(path, orient='records', indent=2)
        exported['state_distribution'] = str(path)

    # Sankey data (already JSON-ready)
    if 'sankey_data' in results and results['sankey_data'] is not None:
        path = output_dir / "sankey_data.json"
        with open(path, 'w') as f:
            json.dump(results['sankey_data'], f, indent=2)
        exported['sankey_data'] = str(path)

    return exported
