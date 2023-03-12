import dedupe

from src.dedupe_api.service import preprocessing, train_dedupe, dedupe_scoring
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


def test_train_dedupe(db):
    deduper = train_dedupe(db, reuse_setting=True)
    assert type(deduper) == dedupe.StaticDedupe


def test_dedupe_scoring(db):
    train_scores, validation_scores, test_score = dedupe_scoring(db)
    assert train_scores is not None
    assert validation_scores is not None
    assert test_score is not None
