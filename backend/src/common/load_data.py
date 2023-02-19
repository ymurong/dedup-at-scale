from src.common.database_util import connection
from src.resources.conf import DOCUMENTS_DATA_PATH
import os


def load_data(DATABASE_URL):
    # import data
    with connection(DATABASE_URL) as conn:
        dir = os.path.dirname(os.path.abspath(__file__))
        documents_csv_path = os.path.join(dir, DOCUMENTS_DATA_PATH)
        import_table_from_csv(conn, "documents", documents_csv_path)


def import_table_from_csv(conn, table_name, csv_path):
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS (SELECT * FROM '{csv_path}')")
