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
def create_spark_session(app_name, spark_master="spark://localhost:7077"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(spark_master) \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()
