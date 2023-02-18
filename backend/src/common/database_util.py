import duckdb
from contextlib import contextmanager
import os


class DBConnector:
    _instance = {}

    def __new__(cls, database_url):
        if database_url not in cls._instance:
            cls._instance[database_url] = super().__new__(cls)
        return cls._instance[database_url]

    def __init__(self, database_url):
        self.conn = duckdb.connect(database=database_url, read_only=False)

    def cursor(self) -> duckdb.DuckDBPyConnection:
        return self.conn.cursor()


@contextmanager
def connection(database_url) -> duckdb.DuckDBPyConnection:
    """
    only used for data initialization
    :param database_url
    """
    conn = duckdb.connect(database=database_url, read_only=False)
    try:
        yield conn
    finally:
        conn.close()


def import_table_from_csv(conn, table_name, csv_path):
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS (SELECT * FROM '{csv_path}')")
