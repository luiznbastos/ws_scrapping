"""
DuckDB-backed drop-in replacement for DatabaseClient.

Reads : DuckDB views over s3://<bucket>/raw/<table>/**/*.parquet (hive-partitioned by run_id)
Writes: DuckDB COPY TO s3://<bucket>/raw/<table>/run_id=<run_id>/<table>.parquet

Keeps the same interface as DatabaseClient so all call sites work unchanged.
DuckDB's httpfs extension picks up AWS credentials from the instance IAM role
(AWS Batch) or from AWS_* environment variables (local / CI).
"""

import logging
from typing import Optional

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# Tables managed as Parquet in S3; each gets a registered view at init time.
MANAGED_TABLES = ["seasons", "monthly_matches", "season_matches"]


class DuckDBClient:
    def __init__(self, bucket: str, run_id: str, aws_region: str = "us-east-1"):
        self.bucket = bucket
        self.run_id = run_id
        self._con = duckdb.connect()
        self._con.execute("INSTALL httpfs; LOAD httpfs;")
        self._con.execute(f"SET s3_region='{aws_region}';")
        for table in MANAGED_TABLES:
            self._register_view(table)

    def _register_view(self, table: str) -> None:
        """Register (or refresh) a DuckDB view that scans all run_id partitions for a table."""
        path = f"s3://{self.bucket}/raw/{table}/**/*.parquet"
        # CREATE VIEW is lazy — it won't fail if no files exist yet.
        self._con.execute(f"""
            CREATE OR REPLACE VIEW {table} AS
            SELECT * FROM read_parquet('{path}', hive_partitioning = true)
        """)
        logger.debug(f"Registered view {table} → {path}")

    # ------------------------------------------------------------------
    # DatabaseClient-compatible interface
    # ------------------------------------------------------------------

    def read_sql(self, query: str, **kwargs) -> pd.DataFrame:
        try:
            return self._con.execute(query).df()
        except Exception as e:
            # Treat "no files" as an empty result — happens on a first run
            # before any Parquet has been written for a given table.
            if "No files found" in str(e) or "Cannot open file" in str(e):
                logger.info(f"No Parquet files found, returning empty DataFrame. Query: {query!r}")
                return pd.DataFrame()
            raise

    def write_df(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
        **kwargs,
    ) -> None:
        if df.empty:
            logger.info(f"Skipped write for {table_name}: empty DataFrame")
            return
        path = f"s3://{self.bucket}/raw/{table_name}/run_id={self.run_id}/{table_name}.parquet"
        self._con.register("_tmp_write", df)
        try:
            self._con.execute(f"COPY _tmp_write TO '{path}' (FORMAT PARQUET)")
            logger.info(f"Wrote {len(df)} rows → {path}")
        finally:
            self._con.unregister("_tmp_write")
        # Refresh the view so subsequent reads in this session include the new partition.
        self._register_view(table_name)

    def fetch_one(self, query: str) -> Optional[tuple]:
        try:
            return self._con.execute(query).fetchone()
        except Exception as e:
            if "No files found" in str(e) or "Cannot open file" in str(e):
                return None
            raise

    def execute_query(self, query: str) -> None:
        self._con.execute(query)
