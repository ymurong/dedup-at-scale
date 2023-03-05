import pandas as pd

from .schemas import TrainingData, RecordDictPair
from pathlib import Path
from .dataset import Dataset
import duckdb

project_root = Path(__file__).parent.parent


class DedupeData:

    def __init__(self, db: duckdb.DuckDBPyConnection, data_path: Path):
        self.data_path = data_path
        self.db = db
        self.training_data = TrainingData(match=[], distinct=[])
        self.input_data = {}
        self.__load__()

    def __load__(self):
        """load dataset"""
        self.dataset = Dataset(conn=self.db, data_path=self.data_path)
        self.__load_training_data__()
        self.__load_input_data__()

    def __load_training_data__(self):
        training_triplets = self.dataset.get_training_triplet()
        for train_triplet in training_triplets:
            ref1_dict, ref2_dict, label = train_triplet
            record = RecordDictPair((ref1_dict, ref2_dict))
            if label is True:
                self.training_data.match.append(record)
            else:
                self.training_data.distinct.append(record)

    def __load_input_data__(self):
        self.input_data = self.dataset.get_collection().to_dict('index')
