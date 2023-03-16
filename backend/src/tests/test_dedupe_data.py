from src.common.dedupe_data import DedupeData
from src.common.schemas import TrainingData, RecordPairs, LabeledRecordPairStr
from typing import Iterator
from types import GeneratorType
import json
import duckdb
import pytest
from pathlib import Path

project_root = Path(__file__).parent.parent


@pytest.fixture
def db():
    conn = duckdb.connect()
    return conn


@pytest.fixture
def dedupe_data(db):
    return DedupeData(db=db, data_path=Path("resources/data"), local=True)


def test_dump_dedupe_data(db, dedupe_data):
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
    input_data = dedupe_data.df_input_data.to_dict("index")
    assert type(input_data) == dict
    assert len(input_data.values()) == 17165


def test_local_preprocessing(db, dedupe_data):
    results = dedupe_data.df_input_data
    assert len(results) == 17165


def test_get_training_data(db, dedupe_data):
    training_data = dedupe_data.training_data
    assert type(training_data) == TrainingData
    assert len(training_data.distinct) == 4027
    assert len(training_data.match) == 3945


def test_get_labeled_training_pairs(db, dedupe_data):
    training_pairs = dedupe_data.get_labeled_training_pairs()
    assert type(training_pairs) == GeneratorType
    assert type(next(training_pairs)) == tuple
    assert len(next(training_pairs)) == 3


def test_get_training_pairs_without_labels(db, dedupe_data):
    training_pairs = dedupe_data.get_training_pairs_without_labels()
    assert type(training_pairs) == GeneratorType
    assert type(next(training_pairs)) == tuple
    assert len(next(training_pairs)) == 2


def test_get_validation_pairs(db, dedupe_data):
    validation_pairs = dedupe_data.get_validation_pairs()
    assert type(validation_pairs) == GeneratorType
    assert type(next(validation_pairs)) == tuple
    assert len(next(validation_pairs)) == 2


def test_get_test_pairs(db, dedupe_data):
    test_pairs = dedupe_data.get_test_pairs()
    assert type(test_pairs) == GeneratorType
    assert type(next(test_pairs)) == tuple
    assert len(next(test_pairs)) == 2
