"""Configuration and parameters for state transition analysis."""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional


# State definitions
STATE_NAMES = {
    1: 'Exploring',
    2: 'Problem-Aware',
    3: 'Purchase-Ready',
    4: 'Purchased'
}

STATE_COLORS = {
    1: '#3498db',  # Blue
    2: '#f39c12',  # Orange
    3: '#9b59b6',  # Purple
    4: '#27ae60',  # Green
}


@dataclass
class Config:
    """Configuration for state transition analysis."""

    # Data source
    data_path: str = 'data/'
    file_pattern: str = '*_SESSIONS_COHORT.csv'

    # Cohort parameters
    cohort_start: Optional[date] = None  # None = use all data
    cohort_end: Optional[date] = None
    cohort_granularity: str = 'W'  # D=daily, W=weekly, M=monthly

    # Analysis parameters
    min_cohort_size: int = 50
    max_sessions_for_sankey: int = 3

    # Traffic source patterns for state assignment
    exploring_traffic_sources: list = field(default_factory=lambda: ['Facebook'])

    # Output
    output_path: str = 'outputs/'
    export_formats: list = field(default_factory=lambda: ['csv', 'json'])

    def __post_init__(self):
        """Validate configuration."""
        valid_granularities = ['D', 'W', 'M']
        if self.cohort_granularity not in valid_granularities:
            raise ValueError(f"cohort_granularity must be one of {valid_granularities}")

        if self.cohort_start and self.cohort_end:
            if self.cohort_start > self.cohort_end:
                raise ValueError("cohort_start must be before cohort_end")


# Default configuration
DEFAULT_CONFIG = Config()
