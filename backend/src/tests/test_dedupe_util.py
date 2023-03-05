from src.common.dedupe_util import DedupeData
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
