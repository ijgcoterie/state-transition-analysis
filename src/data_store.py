"""
Data storage and consolidation utilities.

Handles ingesting new CSV files and maintaining a consolidated data store
that can be queried across time periods.
"""

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Union
import glob
import hashlib

from .config import Config, DEFAULT_CONFIG


class DataStore:
    """
    Manages consolidated session data across multiple CSV imports.

    Supports two backends:
    - SQLite (recommended): Efficient queries, deduplication, no memory limits
    - Parquet: Simple file-based, good for medium datasets
    """

    def __init__(
        self,
        store_path: str = 'data/sessions.db',
        backend: str = 'sqlite',  # 'sqlite' or 'parquet'
    ):
        self.store_path = Path(store_path)
        self.backend = backend
        self.store_path.parent.mkdir(parents=True, exist_ok=True)

        if backend == 'sqlite':
            self._init_sqlite()

    def _init_sqlite(self):
        """Initialize SQLite database with schema."""
        conn = sqlite3.connect(self.store_path)
        cursor = conn.cursor()

        # Main sessions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                SESSION_ID TEXT PRIMARY KEY,
                SESSION_START TIMESTAMP,
                SESSION_END TIMESTAMP,
                SESSION_NUMBER INTEGER,
                USER_ID TEXT,
                USER_PSEUDO_ID TEXT,
                SESSION_PAGE_VIEW_COUNT INTEGER,
                HAS_VIEW_ITEM BOOLEAN,
                HAS_ADD_TO_CART BOOLEAN,
                HAS_BEGIN_CHECKOUT BOOLEAN,
                HAS_PURCHASE BOOLEAN,
                LANDING_PAGE TEXT,
                EXIT_PAGE TEXT,
                SESSION_FIRST_TRAFFIC_SOURCE TEXT,
                SESSION_LAST_TRAFFIC_SOURCE TEXT,
                SESSION_FIRST_TRAFFIC_SOURCE_CHANNEL_GROUPING TEXT,
                SESSION_LAST_TRAFFIC_SOURCE_CHANNEL_GROUPING TEXT,
                _imported_at TIMESTAMP,
                _source_file TEXT
            )
        ''')

        # Index for common queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON sessions(USER_ID)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_session_start ON sessions(SESSION_START)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_session_number ON sessions(SESSION_NUMBER)')

        # Track imported files to avoid re-importing
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS import_log (
                file_hash TEXT PRIMARY KEY,
                file_name TEXT,
                rows_imported INTEGER,
                imported_at TIMESTAMP
            )
        ''')

        conn.commit()
        conn.close()

    def ingest_csv(
        self,
        csv_path: Union[str, Path],
        skip_if_imported: bool = True,
    ) -> dict:
        """
        Ingest a CSV file into the data store.

        Args:
            csv_path: Path to CSV file
            skip_if_imported: Skip if file was already imported

        Returns:
            Dictionary with import stats
        """
        csv_path = Path(csv_path)

        # Calculate file hash for deduplication
        file_hash = self._file_hash(csv_path)

        if skip_if_imported and self._already_imported(file_hash):
            return {
                'status': 'skipped',
                'reason': 'already_imported',
                'file': str(csv_path),
            }

        # Load CSV
        df = pd.read_csv(csv_path, engine='python')
        original_count = len(df)

        if self.backend == 'sqlite':
            stats = self._ingest_to_sqlite(df, csv_path, file_hash)
        else:
            stats = self._ingest_to_parquet(df, csv_path, file_hash)

        stats['file'] = str(csv_path)
        stats['original_rows'] = original_count

        return stats

    def ingest_directory(
        self,
        directory: str = 'data/',
        pattern: str = '*_SESSIONS_COHORT.csv',
        skip_if_imported: bool = True,
    ) -> List[dict]:
        """
        Ingest all matching CSV files from a directory.

        Args:
            directory: Directory to scan
            pattern: Glob pattern for CSV files
            skip_if_imported: Skip files already imported

        Returns:
            List of import stats for each file
        """
        files = glob.glob(str(Path(directory) / pattern))
        results = []

        for f in sorted(files):
            result = self.ingest_csv(f, skip_if_imported)
            results.append(result)
            print(f"  {Path(f).name}: {result.get('status', 'done')} "
                  f"({result.get('new_rows', result.get('reason', '?'))})")

        return results

    def _ingest_to_sqlite(
        self,
        df: pd.DataFrame,
        source_file: Path,
        file_hash: str,
    ) -> dict:
        """Ingest DataFrame to SQLite with deduplication."""
        conn = sqlite3.connect(self.store_path)

        # Add metadata columns
        df = df.copy()
        df['_imported_at'] = datetime.now()
        df['_source_file'] = str(source_file.name)

        # Parse booleans
        bool_cols = ['HAS_VIEW_ITEM', 'HAS_ADD_TO_CART', 'HAS_BEGIN_CHECKOUT', 'HAS_PURCHASE']
        for col in bool_cols:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].str.lower() == 'true'

        # Get existing session IDs to find new ones
        existing = pd.read_sql(
            "SELECT SESSION_ID FROM sessions",
            conn
        )['SESSION_ID'].tolist()

        new_sessions = df[~df['SESSION_ID'].isin(existing)]

        # Insert new sessions
        if len(new_sessions) > 0:
            new_sessions.to_sql('sessions', conn, if_exists='append', index=False)

        # Log the import
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO import_log VALUES (?, ?, ?, ?)",
            (file_hash, str(source_file.name), len(new_sessions), datetime.now())
        )

        conn.commit()
        conn.close()

        return {
            'status': 'success',
            'new_rows': len(new_sessions),
            'duplicate_rows': len(df) - len(new_sessions),
        }

    def _ingest_to_parquet(
        self,
        df: pd.DataFrame,
        source_file: Path,
        file_hash: str,
    ) -> dict:
        """Ingest DataFrame to Parquet with deduplication."""
        parquet_path = self.store_path.with_suffix('.parquet')

        # Add metadata
        df = df.copy()
        df['_imported_at'] = datetime.now()
        df['_source_file'] = str(source_file.name)

        # Parse booleans
        bool_cols = ['HAS_VIEW_ITEM', 'HAS_ADD_TO_CART', 'HAS_BEGIN_CHECKOUT', 'HAS_PURCHASE']
        for col in bool_cols:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = df[col].str.lower() == 'true'

        if parquet_path.exists():
            existing = pd.read_parquet(parquet_path)
            existing_ids = set(existing['SESSION_ID'])
            new_sessions = df[~df['SESSION_ID'].isin(existing_ids)]

            if len(new_sessions) > 0:
                combined = pd.concat([existing, new_sessions], ignore_index=True)
                combined.to_parquet(parquet_path, index=False)
        else:
            new_sessions = df
            df.to_parquet(parquet_path, index=False)

        return {
            'status': 'success',
            'new_rows': len(new_sessions),
            'duplicate_rows': len(df) - len(new_sessions),
        }

    def query(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_ids: Optional[List[str]] = None,
        min_session_number: Optional[int] = None,
        max_session_number: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Query sessions from the data store.

        Args:
            start_date: Filter sessions starting on or after this date (YYYY-MM-DD)
            end_date: Filter sessions starting on or before this date
            user_ids: Filter to specific user IDs
            min_session_number: Minimum session number
            max_session_number: Maximum session number

        Returns:
            DataFrame of matching sessions
        """
        if self.backend == 'sqlite':
            return self._query_sqlite(
                start_date, end_date, user_ids,
                min_session_number, max_session_number
            )
        else:
            return self._query_parquet(
                start_date, end_date, user_ids,
                min_session_number, max_session_number
            )

    def _query_sqlite(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
        user_ids: Optional[List[str]],
        min_session_number: Optional[int],
        max_session_number: Optional[int],
    ) -> pd.DataFrame:
        """Query SQLite database."""
        conn = sqlite3.connect(self.store_path)

        conditions = []
        params = []

        if start_date:
            conditions.append("date(SESSION_START) >= ?")
            params.append(start_date)

        if end_date:
            conditions.append("date(SESSION_START) <= ?")
            params.append(end_date)

        if user_ids:
            placeholders = ','.join('?' * len(user_ids))
            conditions.append(f"USER_ID IN ({placeholders})")
            params.extend(user_ids)

        if min_session_number:
            conditions.append("SESSION_NUMBER >= ?")
            params.append(min_session_number)

        if max_session_number:
            conditions.append("SESSION_NUMBER <= ?")
            params.append(max_session_number)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT * FROM sessions
            WHERE {where_clause}
            ORDER BY USER_ID, SESSION_NUMBER
        """

        df = pd.read_sql(query, conn, params=params)
        conn.close()

        # Parse dates
        df['SESSION_START'] = pd.to_datetime(df['SESSION_START'])
        df['SESSION_END'] = pd.to_datetime(df['SESSION_END'])

        # Convert boolean columns (SQLite stores as 0/1)
        bool_cols = ['HAS_VIEW_ITEM', 'HAS_ADD_TO_CART', 'HAS_BEGIN_CHECKOUT', 'HAS_PURCHASE']
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].astype(bool)

        return df

    def _query_parquet(
        self,
        start_date: Optional[str],
        end_date: Optional[str],
        user_ids: Optional[List[str]],
        min_session_number: Optional[int],
        max_session_number: Optional[int],
    ) -> pd.DataFrame:
        """Query Parquet file."""
        parquet_path = self.store_path.with_suffix('.parquet')

        if not parquet_path.exists():
            return pd.DataFrame()

        df = pd.read_parquet(parquet_path)
        df['SESSION_START'] = pd.to_datetime(df['SESSION_START'])
        df['SESSION_END'] = pd.to_datetime(df['SESSION_END'])

        if start_date:
            df = df[df['SESSION_START'].dt.date >= pd.to_datetime(start_date).date()]

        if end_date:
            df = df[df['SESSION_START'].dt.date <= pd.to_datetime(end_date).date()]

        if user_ids:
            df = df[df['USER_ID'].isin(user_ids)]

        if min_session_number:
            df = df[df['SESSION_NUMBER'] >= min_session_number]

        if max_session_number:
            df = df[df['SESSION_NUMBER'] <= max_session_number]

        return df.sort_values(['USER_ID', 'SESSION_NUMBER'])

    def get_users_in_cohort(
        self,
        cohort_start: str,
        cohort_end: str,
    ) -> List[str]:
        """
        Get user IDs whose first session falls within a date range.

        This is useful for cohort analysis where you want ALL sessions
        for users who started in a specific period.

        Args:
            cohort_start: Start date (YYYY-MM-DD)
            cohort_end: End date (YYYY-MM-DD)

        Returns:
            List of user IDs
        """
        if self.backend == 'sqlite':
            conn = sqlite3.connect(self.store_path)
            query = """
                SELECT DISTINCT USER_ID
                FROM sessions
                WHERE SESSION_NUMBER = 1
                  AND date(SESSION_START) >= ?
                  AND date(SESSION_START) <= ?
            """
            df = pd.read_sql(query, conn, params=[cohort_start, cohort_end])
            conn.close()
            return df['USER_ID'].tolist()
        else:
            df = self.query()
            first_sessions = df[df['SESSION_NUMBER'] == 1]
            mask = (
                (first_sessions['SESSION_START'].dt.date >= pd.to_datetime(cohort_start).date()) &
                (first_sessions['SESSION_START'].dt.date <= pd.to_datetime(cohort_end).date())
            )
            return first_sessions[mask]['USER_ID'].tolist()

    def get_cohort_sessions(
        self,
        cohort_start: str,
        cohort_end: str,
        include_future_sessions: bool = True,
    ) -> pd.DataFrame:
        """
        Get all sessions for users in a cohort.

        This handles the cross-file query problem: even if a user's first
        session is in January but they have sessions in February, this
        returns all their sessions.

        Args:
            cohort_start: Cohort start date (first session on or after)
            cohort_end: Cohort end date (first session on or before)
            include_future_sessions: Include sessions after cohort_end

        Returns:
            DataFrame with all sessions for cohort users
        """
        # Get users in cohort
        user_ids = self.get_users_in_cohort(cohort_start, cohort_end)

        if not user_ids:
            return pd.DataFrame()

        # Get all sessions for these users
        if include_future_sessions:
            return self.query(user_ids=user_ids)
        else:
            return self.query(user_ids=user_ids, end_date=cohort_end)

    def get_stats(self) -> dict:
        """Get statistics about the data store."""
        if self.backend == 'sqlite':
            conn = sqlite3.connect(self.store_path)

            stats = {}
            stats['total_sessions'] = pd.read_sql(
                "SELECT COUNT(*) as n FROM sessions", conn
            )['n'].iloc[0]
            stats['total_users'] = pd.read_sql(
                "SELECT COUNT(DISTINCT USER_ID) as n FROM sessions", conn
            )['n'].iloc[0]
            stats['date_range'] = pd.read_sql(
                "SELECT MIN(date(SESSION_START)) as min_date, MAX(date(SESSION_START)) as max_date FROM sessions",
                conn
            ).iloc[0].to_dict()
            stats['files_imported'] = pd.read_sql(
                "SELECT COUNT(*) as n FROM import_log", conn
            )['n'].iloc[0]

            conn.close()
            return stats
        else:
            df = self.query()
            return {
                'total_sessions': len(df),
                'total_users': df['USER_ID'].nunique(),
                'date_range': {
                    'min_date': str(df['SESSION_START'].min().date()),
                    'max_date': str(df['SESSION_START'].max().date()),
                },
            }

    def _file_hash(self, path: Path) -> str:
        """Calculate hash of file for deduplication."""
        hasher = hashlib.md5()
        with open(path, 'rb') as f:
            # Hash first 1MB for speed
            hasher.update(f.read(1024 * 1024))
        return hasher.hexdigest()

    def _already_imported(self, file_hash: str) -> bool:
        """Check if file was already imported."""
        if self.backend == 'sqlite':
            conn = sqlite3.connect(self.store_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM import_log WHERE file_hash = ?",
                (file_hash,)
            )
            result = cursor.fetchone() is not None
            conn.close()
            return result
        return False
