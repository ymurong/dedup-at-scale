import os

DOCUMENTS_DATA_PATH = "../resources/documents.csv"
RECORDS_EXAMPLE_CSV_PATH = "../resources/csv_example_messy_input.csv"
RECORDS_EXAMPLE_DEDUPE_SETTING_PATH = "../resources/csv_example_learned_settings"
SPARK_MASTER = os.getenv('SPARK_MASTER', default="spark://localhost:7077")