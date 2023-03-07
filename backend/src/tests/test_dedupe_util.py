from src.common.dedupe_util import DedupeData
from src.common.schemas import TrainingData
import json
import duckdb
import pytest
from pathlib import Path


@pytest.fixture
def db():
    conn = duckdb.connect()
    return conn


def test_dump_dedupe_data(db):
    dedupe_data = DedupeData(db=db, data_path=Path("resources/data"))

    # test training data
    training_data_json_string = dedupe_data.training_data.json()
    training_data_json_object = json.loads(training_data_json_string)
    assert dedupe_data.training_data
    assert type(training_data_json_object["match"]) == list
    assert type(training_data_json_object["distinct"]) == list
    assert training_data_json_object["match"][0]["__class__"] == "tuple"
    # value should be a list of two elements (ref1, ref2)
    assert len(training_data_json_object["match"][0]["__value__"]) == 2

    # test input data
    assert type(dedupe_data.input_data) == dict
    assert len(dedupe_data.input_data.values()) == 17165


def test_local_preprocessing(db):
    dedupe_data = DedupeData(db=db, data_path=Path("resources/data"), local=True)
    results = dedupe_data.df_input_data
    assert len(results) == 17165


def test_get_training_data(db):
    dataset = DedupeData(db=db, data_path=Path("resources/data"), local=True)
    training_data = dataset.training_data
    assert type(training_data) == TrainingData
    assert len(training_data.distinct) == 4027
    assert len(training_data.match) == 3945
