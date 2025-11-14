import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional
import os


class DatabaseClient:
    def __init__(self, db_url: Optional[str] = None):
        self.db_url = db_url

    @property
    def engine(self):
        if not hasattr(self, "_engine"):
            if self.db_url is not None:
                self._engine = create_engine(self.db_url)
            else:
                self._engine = create_engine(
                    f"redshift+redshift_connector://{os.environ.get('DB_USER')}:{os.environ.get('DB_PASSWORD')}@{os.environ.get('DB_HOST')}:5439/{os.environ.get('DB_NAME')}"
                )
        return self._engine

    def read_sql(self, query: str, **kwargs) -> pd.DataFrame:
        with self.engine.connect() as connection:
            return pd.read_sql(query, connection, **kwargs)

    def write_df(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
        **kwargs,
    ):
        with self.engine.connect() as connection:
            df.to_sql(
                table_name,
                con=connection,
                if_exists=if_exists,
                index=index,
                **kwargs,
            )

    def execute_query(self, query: str):
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(text(query))

    def fetch_one(self, query: str):
        with self.engine.connect() as connection:
            result = connection.execute(text(query)).fetchone()
            return result
