from pyspark.sql import SparkSession
from contextlib import contextmanager


@contextmanager
def create_spark_session_local():
    sc = SparkSession.builder \
        .master("local") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    try:
        yield sc
    finally:
        sc.stop()


@contextmanager
def create_spark_session(app_name, port=7077):
    sc = SparkSession.builder \
        .appName(app_name) \
        .master(f"spark://localhost:{port}") \
        .getOrCreate()
    try:
        yield sc
    finally:
        sc.stop()
