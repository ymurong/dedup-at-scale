import duckdb
import pytest

from src.common.dataset import Dataset
from src.common.local_preprocessing import inversed_pauthor_ptitle


@pytest.fixture
def db():
    conn = duckdb.connect()
    conn.install_extension("json")
    conn.load_extension("json")
    return conn


def test_dataset_load(db):
    dataset = Dataset(conn=db, data_path="resources/data")
    total_refs = dataset.get_total_references()
    total_ptypes = dataset.get_total_ptypes()
    total_train = dataset.get_total_train()
    assert total_refs[0] == 17165
    assert total_ptypes[0] == 9
    assert total_train[0] == 7972


def test_dataset_get_input(db):
    dataset = Dataset(conn=db, data_path="resources/data")
    results = dataset.get_collection()
    assert len(results) == 17165
