from src.dedupe_api.service import preprocessing, train_predicates
from src.common.spark_util import if_spark_running
import duckdb
import pytest
import pandas as pd


@pytest.fixture
def db():
    conn = duckdb.connect()
    return conn


def test_local_preprocessing(db):
    result = preprocessing(db, local=True)
    assert type(result) == pd.DataFrame
    assert len(result) == 17165


@pytest.mark.skipif(if_spark_running() is False, reason="spark is not running")
def test_spark_preprocessing(db):
    result = preprocessing(db, local=False, slicer=slice(None, 100, None))
    assert type(result) == pd.DataFrame
    assert len(result) == 100


def test_train_predicates(db):
    predicates = train_predicates(db, reuse_setting=True)
    assert type(predicates) == list
