from src.dedupe_api.service import preprocessing
import json
import duckdb
import pytest
import pandas as pd
from pathlib import Path


@pytest.fixture
def db():
    conn = duckdb.connect()
    return conn


def test_preprocessing(db):
    result = preprocessing(db)
    assert type(result) == pd.DataFrame
