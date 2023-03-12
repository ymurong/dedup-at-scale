import duckdb
import pytest
from src.common.dataset import Dataset
from pathlib import Path


@pytest.fixture
def db():
    conn = duckdb.connect()
    conn.install_extension("json")
    conn.load_extension("json")
    return conn


@pytest.fixture
def dataset(db):
    return Dataset(conn=db, data_path=Path("resources/data"))


def test_dataset_load(db, dataset):
    total_refs = dataset.get_total_references()
    total_ptypes = dataset.get_total_ptypes()
    total_train = dataset.get_total_train()
    total_validation = dataset.get_total_validation()
    total_test = dataset.get_total_test()
    assert total_refs[0] == 17165
    assert total_ptypes[0] == 9
    assert total_train[0] == 7972
    assert total_validation[0] == 994
    assert total_test[0] == 1034


def test_dataset_get_input(db, dataset):
    results = dataset.get_collection()
    assert len(results) == 17165


def test_get_validation_data(db, dataset):
    results = dataset.execute("""
        SELECT * FROM validation;
    """)
    assert results.get("key1") is not None
    assert results.get("key2") is not None
