import os

# Spark
SPARK_MASTER = os.getenv('SPARK_MASTER', default="spark://localhost:7077")

# Example APP
DOCUMENTS_DATA_PATH = "../resources/documents.csv"
RECORDS_EXAMPLE_CSV_PATH = "../resources/csv_example_messy_input.csv"
RECORDS_EXAMPLE_DEDUPE_SETTING_PATH = "../resources/csv_example_learned_settings"

# Dedupe App
DATA_PATH = "resources/data"
DEDUPE_SETTING_PATH = "resources/data/dedupe_learned_settings"

BLOCKING_FIELDS_FULL = [
    {'field': 'pauthor', 'type': 'String', 'has missing': True},  # it can be improved as set of authors
    {'field': 'peditor', 'type': 'String', 'has missing': True},
    {'field': 'ptitle', 'type': 'String', 'has missing': True},
    {'field': 'pyear', 'type': 'Exact', 'has missing': True},
    {'field': 'paddress', 'type': 'String', 'has missing': True},
    {'field': 'ppublisher', 'type': 'String', 'has missing': True},
    {'field': 'pseries', 'type': 'String', 'has missing': True},
    {'field': 'pjournal', 'type': 'String', 'has missing': True},
    {'field': 'pbooktitle', 'type': 'String', 'has missing': True},
    {'field': 'ptype', 'type': 'String', 'has missing': True},
]
