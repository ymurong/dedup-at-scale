import logging
import numpy as np
import pandas as pd
from .schemas import TrainingData, RecordDictPair, RecordSet, RecordPairs, LabeledRecordPairStr
from pathlib import Path
from .dataset import Dataset
from .local_preprocessing import inversed_pauthor_ptitle, abs_year, string_normalize, pauthor_to_set
from typing import List, Tuple
from src.resources.conf import DATA_PATH
from src.common.exception import missing_clean_collection_data_exception
import duckdb
import json
from typing import Iterator

project_root = Path(__file__).parent.parent
logger = logging.getLogger(__name__)


class DedupeData:

    def __init__(self, db: duckdb.DuckDBPyConnection, data_path: Path, local=True):
        """
        Load dataset, if local is True, then DedupeData class will preprocess it locally,
        otherwise unpreprocessed input_data will be set
        """
        self.data_path = data_path
        self.db = db
        self.local_preprocessing = local
        self.training_data = TrainingData(match=[], distinct=[])
        self.df_input_data: pd.DataFrame = None
        self._training_triplets = None
        self._load()

    def _load(self):
        self.dataset = Dataset(conn=self.db, data_path=self.data_path)
        self._load_input_data()
        if self.local_preprocessing:
            self._local_preprocessing()
        self._dump_input_data()
        self._load_training_data()
        self._dump_training_data()

    def _load_input_data(self):
        self.df_input_data = self.dataset.get_collection()

    def _local_preprocessing(self):
        """Transform the data and write to resources/data"""
        self.df_input_data = self.df_input_data.transform(inversed_pauthor_ptitle).transform(
            abs_year).transform(string_normalize)

    def _dump_input_data(self):
        self.df_input_data.to_csv(project_root / DATA_PATH / "collection.csv", index=True, header=True)

    def _get_training_triplet(self) -> List[Tuple[dict, dict, bool]]:
        """Return the labeled training triplets (ref1, ref2, label)"""
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
        nb_features = (len(columns) - 2) // 2
        column_names = columns[1:1 + nb_features]
        for train_sample in training_pairs.values:
            ref1 = train_sample[1:1 + nb_features]
            ref1_dict = pd.Series(data=ref1, index=column_names).to_dict()
            ref2 = train_sample[nb_features + 1:2 * nb_features + 1]
            ref2_dict = pd.Series(data=ref2, index=column_names).to_dict()
            label = train_sample[-1]
            triplets.append((ref1_dict, ref2_dict, label))
        return triplets

    def _load_training_data(self):
        """load training data for dedupe training"""
        self._training_triplets = self._get_training_triplet()
        for train_triplet in self._training_triplets:
            ref1_dict, ref2_dict, label = train_triplet
            ref1_dict["pauthor"] = RecordSet(tuple(ref1_dict["pauthor"].split("|")))
            ref2_dict["pauthor"] = RecordSet(tuple(ref2_dict["pauthor"].split("|")))
            record = RecordDictPair((ref1_dict, ref2_dict))
            if label is True:
                self.training_data.match.append(record)
            else:
                self.training_data.distinct.append(record)

    def _dump_training_data(self):
        training_data_json_object = json.loads(self.training_data.json())
        with open(project_root / "resources/data/training_data.json", mode="w") as file:
            file.write(json.dumps(training_data_json_object, ensure_ascii=True, indent=2))

    def _unlabeled_pairs(self, table: str) -> RecordPairs:
        """Return an iterator of RecordPairs given validation table or test table"""
        pairs = self.db.execute(f"""
                               select distinct on (t.column0) t.column0, 
                               d.pid, d.pkey, d.pauthor,d.peditor,d.ptitle,d.pyear,d.paddress,d.ppublisher,d.pseries, d.pjournal,d.pjournalfull, d.pbooktitle, d.pbooktitlefull, d.ptype,
                               d2.pid, d2.pkey, d2.pauthor,d2.peditor,d2.ptitle,d2.pyear,d2.paddress,d2.ppublisher,d2.pseries, d2.pjournal,d2.pjournalfull, d2.pbooktitle, d2.pbooktitlefull, d2.ptype
                               from {table} t
                               inner join collection d on lower(t.key1) = d.pkey
                               inner join collection d2 on lower(t.key2) = d2.pkey
                               order by t.column0 ASC
                           """).df()
        pairs = pairs.replace({np.nan: None})
        columns = pairs.columns
        nb_features = (len(columns) - 1) // 2
        column_names = columns[1:1 + nb_features]
        for validation_sample in pairs.values:
            ref1 = validation_sample[1:1 + nb_features]
            ref1_dict = pd.Series(data=ref1, index=column_names).to_dict()
            ref2 = validation_sample[nb_features + 1:2 * nb_features + 1]
            ref2_dict = pd.Series(data=ref2, index=column_names).to_dict()
            ref1_dict["pauthor"] = tuple(ref1_dict["pauthor"].split("|"))
            ref2_dict["pauthor"] = tuple(ref2_dict["pauthor"].split("|"))
            yield (
                (ref1_dict["pkey"], ref1_dict),
                (ref2_dict["pkey"], ref2_dict)
            )

    def get_labeled_training_pairs(self) -> Iterator[LabeledRecordPairStr]:
        """This generates iterator of Training RecordPairs with labels, useful for manual training without dedupe"""
        for train_triplet in self._training_triplets:
            ref1_dict, ref2_dict, label = train_triplet
            yield (
                (ref1_dict["pkey"], ref1_dict),
                (ref2_dict["pkey"], ref2_dict),
                label
            )

    def get_training_pairs_without_labels(self) -> RecordPairs:
        """Return an iterator of Training RecordPairs"""
        return self._unlabeled_pairs("train")

    def get_validation_pairs(self) -> RecordPairs:
        """Return an iterator of Validation RecordPairs"""
        return self._unlabeled_pairs("validation")

    def get_test_pairs(self) -> RecordPairs:
        """Return an iterator of Test RecordPairs"""
        return self._unlabeled_pairs("test")
