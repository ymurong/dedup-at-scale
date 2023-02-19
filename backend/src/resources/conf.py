import os

DOCUMENTS_DATA_PATH = "../resources/documents.csv"
SPARK_MASTER = os.getenv('SPARK_MASTER', default="spark://localhost:7077")