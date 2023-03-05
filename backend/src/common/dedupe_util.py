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
        training_pairs = self.dataset.get_training_pairs()
        columns = training_pairs.columns
        nb_features = (len(columns) - 1) // 2
        column_names = columns[1:1 + nb_features]
        for train_sample in training_pairs.values:
            ref1 = train_sample[1:1 + nb_features]
            ref1_dict = pd.Series(data=ref1, index=column_names).to_dict()
            ref2 = train_sample[nb_features + 1:2 * nb_features + 1]
            ref2_dict = pd.Series(data=ref2, index=column_names).to_dict()
            label = train_sample[-1]
            record = RecordDictPair((ref1_dict, ref2_dict))
            if label is True:
                self.training_data.match.append(record)
            else:
                self.training_data.distinct.append(record)

    def __load_input_data__(self):
        self.input_data = self.dataset.get_input().to_dict('index')
