from src.dedupe_api.custom_dedupe import CustomDedupe
import duckdb
import pytest


@pytest.fixture
def db():
    conn = duckdb.connect()
    return conn


def test_clustering(db):
    custom_dedupe = CustomDedupe(db)
    clusters = custom_dedupe(classifier_name="LogisticRegression").clustering().clusters
    assert clusters is not None
