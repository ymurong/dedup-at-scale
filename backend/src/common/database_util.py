import duckdb
from contextlib import contextmanager


@contextmanager
def connection(database_url):
    conn = duckdb.connect(database=database_url, read_only=False)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def cursor(database_url):
    with connection(database_url) as conn:
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()


def import_table_from_csv(conn, table_name, csv_path):
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS (SELECT * FROM '{csv_path}')")
