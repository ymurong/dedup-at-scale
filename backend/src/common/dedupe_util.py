import logging

import pandas as pd
from .schemas import TrainingData, RecordDictPair
from pathlib import Path
from .dataset import Dataset
from .local_preprocessing import lower_case, inversed_pauthor_ptitle, abs_year, string_normalize
from typing import List, Tuple
from src.resources.conf import DATA_PATH
from src.common.exception import missing_clean_collection_data_exception
import duckdb

project_root = Path(__file__).parent.parent
logger = logging.getLogger(__name__)


class DedupeData:
    """
    if local is True, then DedupeData class will preprocess it locally,
    otherwise unpreprocessed input_data will be returned
    """

    def __init__(self, db: duckdb.DuckDBPyConnection, data_path: Path, local=True):
        self.data_path = data_path
        self.db = db
        self.training_data = TrainingData(match=[], distinct=[])
        self.input_data: dict = None
        self.df_input_data: pd.DataFrame = None
        self.local_preprocessing = local
        self.__load__()
        self.input_data = self.df_input_data.to_dict('index')

    def __load__(self):
        """load dataset"""
        self.dataset = Dataset(conn=self.db, data_path=self.data_path)
        self.__load_input_data__()
        if self.local_preprocessing:
            self.__local_preprocessing__()
        self.__load_training_data__()

    def __load_input_data__(self):
        self.df_input_data = self.dataset.get_collection()

    def __local_preprocessing__(self):
        """transform the data and write to resources/data"""
        self.df_input_data = self.df_input_data.transform(inversed_pauthor_ptitle).transform(
            abs_year).transform(string_normalize)
        self.df_input_data.to_csv(project_root / DATA_PATH / "collection.csv", index=True, header=True)

    def __load_training_data__(self):
        training_triplets = self.__get_training_triplet__()
        for train_triplet in training_triplets:
            ref1_dict, ref2_dict, label = train_triplet
            record = RecordDictPair((ref1_dict, ref2_dict))
            if label is True:
                self.training_data.match.append(record)
            else:
                self.training_data.distinct.append(record)

    def __get_training_triplet__(self) -> List[Tuple[dict, dict, bool]]:
        """
        return the labeled training triplets [ref1, ref2, label]
        """
        cleaned_collection = project_root / self.data_path / 'collection.csv'
        if self.local_preprocessing is True or cleaned_collection.is_file():
            self.db.execute(f"""
               CREATE OR REPLACE TABLE collection AS SELECT * FROM read_csv_auto('{cleaned_collection}');
               """)
        else:
            logger.error("Cleaned collection data is missing under resources/data directory!")
            raise missing_clean_collection_data_exception

        triplets = []
        training_pairs = self.db.execute(f"""
                       select distinct on (t.column0) t.column0, 
                       d.pid, d.pkey, d.pauthor,d.peditor,d.ptitle,d.pyear,d.paddress,d.ppublisher,d.pseries, d.pjournal,d.pbooktitle, d.ptype,
                       d2.pid, d2.pkey, d2.pauthor,d2.peditor,d2.ptitle,d2.pyear,d2.paddress,d2.ppublisher,d2.pseries, d2.pjournal, d2.pbooktitle, d2.ptype,
                       t.label
                       from train t 
                       inner join collection d on lower(t.key1) = d.pkey
                       inner join collection d2 on lower(t.key2) = d2.pkey
                   """).df()
        columns = training_pairs.columns
        nb_features = (len(columns) - 1) // 2
        column_names = columns[1:1 + nb_features]
        for train_sample in training_pairs.values:
            ref1 = train_sample[1:1 + nb_features]
            ref1_dict = pd.Series(data=ref1, index=column_names).to_dict()
            ref2 = train_sample[nb_features + 1:2 * nb_features + 1]
            ref2_dict = pd.Series(data=ref2, index=column_names).to_dict()
            label = train_sample[-1]
            triplets.append((ref1_dict, ref2_dict, label))
        return triplets
