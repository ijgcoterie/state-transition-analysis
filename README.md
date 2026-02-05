# State Transition Analysis

Analyze how users move between intent states across their session lifecycle. Supports cohort analysis, channel breakdowns, and transition tracking across multiple time periods.

## State Definitions

| State | Name | Trigger Logic |
|-------|------|---------------|
| 1 | Exploring | First-time visitors from Facebook (default) |
| 2 | Problem-Aware | Returning visitor (session > 1) OR non-Facebook traffic source |
| 3 | Purchase-Ready | `HAS_ADD_TO_CART = True` OR `HAS_BEGIN_CHECKOUT = True` |
| 4 | Purchased | `HAS_PURCHASE = True` |

States are hierarchical (4 > 3 > 2 > 1). Each session is assigned the **highest** state achieved.

## Installation

```bash
cd state_transition_analysis
pip install -r requirements.txt
```

## Quick Start

```bash
# 1. Ingest your CSV data
python ingest_data.py data/

# 2. Run analysis
python run_analysis.py --start 2026-01-01 --end 2026-01-31
```

## Project Structure

```
state_transition_analysis/
├── data/                      # Data directory
│   ├── *.csv                  # Raw CSV files (input)
│   └── sessions.db            # Consolidated SQLite database
├── notebooks/
│   └── exploration.ipynb      # Interactive analysis notebook
├── outputs/                   # Generated outputs
│   ├── state_entry_table.csv
│   ├── cohort_metrics.csv
│   ├── channel_breakdown.csv
│   ├── transition_matrix.csv
│   ├── sankey_data.json
│   └── run_summary.json
├── src/                       # Core library
│   ├── config.py              # Configuration and state definitions
│   ├── data_loader.py         # Load/validate data
│   ├── data_store.py          # SQLite/Parquet data consolidation
│   ├── state_assignment.py    # Vectorized state logic
│   ├── metrics.py             # Metric calculations
│   └── export.py              # Export utilities
├── ingest_data.py             # CLI: Ingest new CSV files
├── run_analysis.py            # CLI: Run analysis
└── requirements.txt
```

## Data Workflow

```
                  ┌─────────────────┐
                  │  New CSV Files  │
                  │  (monthly data) │
                  └────────┬────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  python ingest_data.py │
              │  • Deduplicates by     │
              │    SESSION_ID          │
              │  • Tracks imports      │
              └────────────┬───────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  sessions.db    │
                  │  (SQLite)       │
                  │  Consolidated   │
                  └────────┬────────┘
                           │
                           ▼
              ┌────────────────────────┐
              │  python run_analysis.py│
              │  • Query any cohort    │
              │  • Cross-file sessions │
              └────────────────────────┘
```

### Handling Multiple CSV Files

The system automatically handles users whose sessions span multiple CSV files:

1. **Ingest** consolidates all CSVs into a single SQLite database
2. **Cohort queries** find users by first session date, then retrieve ALL their sessions
3. **Deduplication** ensures the same session isn't counted twice

Example: A user's first session is in January (from `jan_sessions.csv`), but they return in February (from `feb_sessions.csv`). When you analyze the January cohort, both sessions are included.

## CLI Reference

### ingest_data.py

Ingest CSV files into the consolidated data store.

```bash
# Ingest all CSVs in data/ directory
python ingest_data.py

# Ingest specific file
python ingest_data.py path/to/file.csv

# Force re-import (even if already imported)
python ingest_data.py --force

# Use Parquet backend instead of SQLite
python ingest_data.py --backend parquet
```

**Options:**
| Flag | Default | Description |
|------|---------|-------------|
| `source` | `data/` | CSV file or directory to ingest |
| `--backend`, `-b` | `sqlite` | Storage backend: `sqlite` or `parquet` |
| `--store`, `-s` | `data/sessions.db` | Path to data store |
| `--force`, `-f` | False | Re-import files even if already imported |
| `--pattern`, `-p` | `*_SESSIONS_COHORT.csv` | Glob pattern for CSV files |

### run_analysis.py

Run state transition analysis.

```bash
# Analyze specific cohort
python run_analysis.py --start 2026-01-01 --end 2026-01-31

# Monthly cohorts with verbose output
python run_analysis.py --start 2026-01-01 --end 2026-06-30 --granularity M -v

# Export dashboard-ready JSON files
python run_analysis.py --dashboard

# All options
python run_analysis.py \
  --data data/ \
  --start 2026-01-01 \
  --end 2026-01-31 \
  --granularity W \
  --min-cohort-size 50 \
  --output outputs/ \
  --dashboard \
  --verbose
```

**Options:**
| Flag | Default | Description |
|------|---------|-------------|
| `--data`, `-d` | `data/` | Path to data file, directory, or SQLite DB |
| `--start`, `-s` | None | Cohort start date (YYYY-MM-DD) |
| `--end`, `-e` | None | Cohort end date (YYYY-MM-DD) |
| `--granularity`, `-g` | `W` | Cohort period: `D` (daily), `W` (weekly), `M` (monthly) |
| `--output`, `-o` | `outputs/` | Output directory |
| `--min-cohort-size` | `50` | Minimum users per cohort for reporting |
| `--dashboard` | False | Export dashboard-optimized JSON files |
| `--verbose`, `-v` | False | Print detailed progress |

## Interactive Exploration

Open `notebooks/exploration.ipynb` for interactive analysis:

```python
from src import Config, DataStore, assign_states
from src.metrics import calculate_cohort_metrics, calculate_channel_metrics

# Configure
config = Config(
    cohort_start=date(2026, 1, 1),
    cohort_end=date(2026, 3, 31),
    cohort_granularity='W',
)

# Load from data store
store = DataStore('data/sessions.db')
df = store.get_cohort_sessions('2026-01-01', '2026-03-31')

# Assign states and analyze
df = assign_states(df, config)
cohort_metrics = calculate_cohort_metrics(df, config)
```

## Configuration

Edit `src/config.py` to customize:

```python
# State definitions
STATE_NAMES = {
    1: 'Exploring',
    2: 'Problem-Aware',
    3: 'Purchase-Ready',
    4: 'Purchased'
}

# Default configuration
@dataclass
class Config:
    cohort_start: Optional[date] = None
    cohort_end: Optional[date] = None
    cohort_granularity: str = 'W'  # D, W, or M
    min_cohort_size: int = 50
    exploring_traffic_sources: list = field(default_factory=lambda: ['Facebook'])
```

### Modifying State Logic

Edit `src/state_assignment.py` to change state assignment rules:

```python
def assign_states(df, config):
    conditions = [
        # State 4: Purchased
        df['HAS_PURCHASE'].fillna(False),

        # State 3: Purchase-Ready
        (df['HAS_ADD_TO_CART'].fillna(False) | df['HAS_BEGIN_CHECKOUT'].fillna(False)),

        # State 2: Problem-Aware (customize this)
        (df['SESSION_NUMBER'] > 1) | ~is_exploring_source,
    ]
    choices = [4, 3, 2]
    df['STATE'] = np.select(conditions, choices, default=1)
```

## Output Files

### state_entry_table.csv
Session-level data with state assignments.

| Column | Description |
|--------|-------------|
| SESSION_ID | Unique session identifier |
| USER_ID | User identifier |
| SESSION_NUMBER | Sequential session number for user |
| STATE | Assigned state (1-4) |
| STATE_NAME | State name |
| COHORT_PERIOD | Cohort period (e.g., "2026-01") |

### cohort_metrics.csv
Metrics aggregated by cohort period.

| Column | Description |
|--------|-------------|
| COHORT_PERIOD | Cohort period |
| total_users | Users in cohort |
| purchased_rate | % who purchased |
| purchase_ready_rate | % who reached purchase-ready |
| return_rate | % who returned for 2+ sessions |

### channel_breakdown.csv
Metrics by first-touch acquisition channel.

| Column | Description |
|--------|-------------|
| FIRST_TOUCH_CHANNEL | Acquisition channel |
| total_users | Users from channel |
| purchase_rate | % who purchased |
| purchase_ready_rate | % who reached purchase-ready |
| avg_sessions | Average sessions per user |

### transition_matrix.csv
State-to-state transition rates (%).

### sankey_data.json
Flow visualization data with nodes and links for Sankey diagrams.

## Scheduled Runs

For automated daily/weekly analysis:

```bash
#!/bin/bash
# scheduled_analysis.sh

cd /path/to/state_transition_analysis

# Ingest any new CSV files
python ingest_data.py

# Run analysis for recent cohorts
python run_analysis.py \
  --start $(date -v-30d +%Y-%m-%d) \
  --end $(date +%Y-%m-%d) \
  --granularity D \
  --dashboard

# Optional: sync outputs to dashboard/S3/etc.
# aws s3 sync outputs/ s3://your-bucket/state-analysis/
```

Add to cron:
```bash
# Run daily at 6 AM
0 6 * * * /path/to/scheduled_analysis.sh >> /var/log/state_analysis.log 2>&1
```

## Expected CSV Format

Input CSV files should have these columns:

| Column | Type | Description |
|--------|------|-------------|
| SESSION_ID | string | Unique session identifier |
| SESSION_START | timestamp | Session start time |
| SESSION_END | timestamp | Session end time |
| SESSION_NUMBER | int | Sequential session number (1 = first) |
| USER_ID | string | User identifier |
| HAS_VIEW_ITEM | boolean | Viewed a product |
| HAS_ADD_TO_CART | boolean | Added to cart |
| HAS_BEGIN_CHECKOUT | boolean | Started checkout |
| HAS_PURCHASE | boolean | Completed purchase |
| LANDING_PAGE | string | First page URL |
| SESSION_FIRST_TRAFFIC_SOURCE_CHANNEL_GROUPING | string | Traffic source channel |

## Troubleshooting

### CSV parsing errors
If you get `ParserError: Error tokenizing data`, your CSV has multi-line fields. The data loader uses `engine='python'` to handle this automatically.

### Memory issues with large datasets
Use the SQLite data store instead of loading CSVs directly:
```bash
python ingest_data.py  # Consolidate to SQLite first
python run_analysis.py  # Queries from SQLite
```

### Missing sessions for returning users
Ensure you've ingested ALL relevant CSV files before running cohort analysis. The system only includes sessions that exist in the data store.
