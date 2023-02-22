from src.common.database_util import connection
from src.resources.conf import DOCUMENTS_DATA_PATH
import os
import re
from unidecode import unidecode
import csv


def load_data(DATABASE_URL):
    # import data
    with connection(DATABASE_URL) as conn:
        dir = os.path.dirname(os.path.abspath(__file__))
        documents_csv_path = os.path.join(dir, DOCUMENTS_DATA_PATH)
        import_table_from_csv(conn, "documents", documents_csv_path)


def import_table_from_csv(conn, table_name, csv_path):
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS (SELECT * FROM '{csv_path}')")


def to_abs_fname(relative_path):
    dir = os.path.dirname(os.path.abspath(__file__))
    fname = os.path.join(dir, relative_path)
    return fname


def read_data(filename):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID and each value is dict
    """

    def pre_process(column):
        """
        Do a little bit of data cleaning with the help of Unidecode and Regex.
        Things like casing, extra spaces, quotes and new lines can be ignored.
        """
        column = unidecode(column)
        column = re.sub('  +', ' ', column)
        column = re.sub('\n', ' ', column)
        column = column.strip().strip('"').strip("'").lower().strip()
        # If data is missing, indicate that by setting the value to `None`
        if not column:
            column = None
        return column

    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_row = [(k, pre_process(v)) for (k, v) in row.items()]
            row_id = int(row['Id'])
            data_d[row_id] = dict(clean_row)

    return data_d
