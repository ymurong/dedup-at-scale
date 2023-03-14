import logging
import pandas as pd
from duckdb import DuckDBPyConnection
from src.common.spark_util import create_spark_session
from typing import TypeVar, Union
from src.resources.conf import SPARK_MASTER, DATA_PATH, DEDUPE_SETTING_PATH
from src.dedupe_api.exception import spark_execution_exception, dedupe_missing_setting_exception
from src.common.dedupe_util import DedupeData
from src.common.spark_preprocessing import lower_case, abs_year, inversed_pauthor_ptitle, null_type_fix
from pathlib import Path
from src.common.local_preprocessing import pauthor_to_set
import dedupe
from itertools import chain
from sklearn.base import BaseEstimator
from sklearn.metrics import accuracy_score
import numpy
import os

Scores = Union[numpy.memmap, numpy.ndarray]

logger = logging.getLogger(__name__)
project_root = Path(__file__).parent.parent

TCustomDedupe = TypeVar("TCustomDedupe", bound="CustomDedupe")


class CustomDedupe:
    def __init__(self, conn: DuckDBPyConnection, local_preprocessing=True):
        self.conn = conn
        self.local_preprocessing = local_preprocessing
        self.dedupe_data = DedupeData(db=self.conn, data_path=DATA_PATH, local=self.local_preprocessing)
        self.settings_file = project_root / DEDUPE_SETTING_PATH
        self.df_collection: pd.DataFrame = None
        self.deduper: dedupe.StaticDedupe = None
        self.df_train_scores: pd.DataFrame = None
        self.df_validation_scores: pd.DataFrame = None
        self.df_test_scores: pd.DataFrame = None
        self.clusters = None

    def __call__(self, classifier_name="LogisticRegression", reuse_setting=True) -> TCustomDedupe:
        self.settings_file = project_root / (DEDUPE_SETTING_PATH + f"_{classifier_name}")
        self.reuse_setting = reuse_setting
        return self

    @staticmethod
    def _memmap_iterator(memmap):
        for (key1, key2), v in memmap:
            yield key1, key2, v

    @staticmethod
    def _cleanup_scores(arr: Scores) -> None:
        try:
            mmap_file = arr.filename  # type: ignore
        except AttributeError:
            pass
        else:
            arr._mmap.close()  # type: ignore # Unmap file to prevent PermissionError when deleting temp file
            del arr
            if mmap_file:
                os.remove(mmap_file)

    @staticmethod
    def _add_singletons(all_ids, clusters):
        singletons = set(all_ids)

        for record_ids, score in clusters:
            singletons.difference_update(record_ids)
            yield (record_ids, score)

        for singleton in singletons:
            yield (singleton,), (1.0,)

    def preprocessing(self, slicer=slice(None, None, None)) -> TCustomDedupe:
        """
        if local is True, then return already precessed data from DedupeData,
        otherwise unpreprocessed data will be fed to spark (files would be stored in docker containers for now...)
        """
        # ## load data
        self.df_collection = self.dedupe_data.df_input_data[slicer]

        if self.local_preprocessing:
            return self

        # create spark session
        try:
            with create_spark_session("dedupe_preprocessing", spark_master=SPARK_MASTER) as spark:
                # spark computation
                sdf_collection = spark.createDataFrame(self.df_collection).repartition(8, "pid")
                sdf_collection = inversed_pauthor_ptitle(spark, sdf_collection)
                sdf_collection = sdf_collection.transform(lower_case) \
                    .transform(abs_year)
                sdf_collection2 = sdf_collection.transform(null_type_fix)
                # parquet will write to one of the worker
                sdf_collection2.coalesce(1).write.mode("overwrite").parquet("file:///tmp/collection_parquet")
        except Exception as e:
            logger.error(e, exc_info=True)
            raise spark_execution_exception
        return self

    def train(self, reuse_setting=True, classifier: BaseEstimator = None) -> TCustomDedupe:
        """
        Train blocking rules and classifier, save it to setting file for later use, and return the trained predicates
        :param conn: duckDBConnection
        :param reuse_setting: If True then the existing setting (classifier and predicates) will be used
        :param classifier: sklearn classifier instance,
        :return: trained dedupe.StaticDedupe instance
        """

        if classifier is not None:
            self.settings_file = project_root / (DEDUPE_SETTING_PATH + f"_{classifier.__class__.__name__}")

        # if reuse setting then we will load the existing setting file and prevent from training again
        if reuse_setting:
            if self.settings_file.is_file():
                with open(self.settings_file, 'rb') as f:
                    self.deduper = dedupe.StaticDedupe(f)
                    return self
            else:
                raise dedupe_missing_setting_exception

        # retrain the model, regenerate setting file and return the predicates
        # ## load data
        input_data: dict = self.dedupe_data.df_input_data.transform(pauthor_to_set).to_dict('index')
        training_file = project_root / "resources/data/training_data.json"

        # ## train dedupe

        def pauthors(input_data):
            for record in input_data.values():
                yield record['pauthor']

        BLOCKING_FIELDS = [
            {'field': 'pauthor', 'type': 'Set', 'corpus': pauthors(input_data)},
            {'field': 'ptitle', 'type': 'String'},
            # {'field': 'pyear', 'type': 'Exact', 'has missing': True},
            #{'field': 'pjournal', 'type': 'String', 'has missing': True},
            {'field': 'pbooktitle', 'type': 'String', 'has missing': True},
            #{'field': 'ptype', 'type': 'String', 'has missing': True}
        ]

        # Create a new deduper object and pass our data model to it.
        self.deduper = dedupe.Dedupe(BLOCKING_FIELDS)

        # be default, the classifier is a L2 regularized logistic regression classifier.
        if classifier is not None:
            self.deduper.classifier = classifier

        with open(training_file, "r") as f_training_data:
            self.deduper.prepare_training(input_data, f_training_data, sample_size=10000, blocked_proportion=.9)

        # use 'y', 'n' and 'u' keys to flag duplicates
        # press 'f' when you are finished
        # print('starting active labeling...')
        # dedupe.console_label(deduper)

        # Using the examples we just labeled, train the deduper and learn

        # blocking predicates
        logger.info("dedupe start training predicates... ")
        self.deduper.train()

        # When finished, save our training away to disk
        with open(training_file, 'w') as tf:
            self.deduper.write_training(tf)

        # Save our setting file to disk
        logger.info("writing dedupe setting file to disk ... ")
        with open(self.settings_file, 'wb') as sf:
            self.deduper.write_settings(sf)

        # load static dedupe and return
        with open(self.settings_file, 'rb') as f:
            self.deduper = dedupe.StaticDedupe(f)
        return self

    def scoring(self) -> TCustomDedupe:
        if not self.settings_file.is_file():
            raise dedupe_missing_setting_exception

        with open(self.settings_file, 'rb') as f:
            self.deduper = dedupe.StaticDedupe(f)

        train_scores = self.deduper.score(self.dedupe_data.get_training_pairs_without_labels())
        validation_scores = self.deduper.score(self.dedupe_data.get_validation_pairs())
        test_scores = self.deduper.score(self.dedupe_data.get_test_pairs())

        columns = ["key1", "key2", "score"]
        self.df_train_scores = pd.DataFrame(list(self._memmap_iterator(train_scores)), columns=columns)
        self.df_validation_scores = pd.DataFrame(list(self._memmap_iterator(validation_scores)), columns=columns)
        self.df_test_scores = pd.DataFrame(list(self._memmap_iterator(test_scores)), columns=columns)

        self.df_train_scores.to_csv(project_root / DATA_PATH / "train_scoring_results.csv", index=True)
        self.df_validation_scores.to_csv(project_root / DATA_PATH / "validation_hidden_scoring_results.csv", index=True)
        self.df_test_scores.to_csv(project_root / DATA_PATH / "test_hidden_scoring_results.csv", index=True)

        return self

    def clustering(self) -> TCustomDedupe:
        if not self.settings_file.is_file():
            raise dedupe_missing_setting_exception
        with open(self.settings_file, 'rb') as f:
            self.deduper = dedupe.StaticDedupe(f)
            pair_scores = self.deduper.score(chain(self.dedupe_data.get_training_pairs_without_labels(),
                                                   self.dedupe_data.get_validation_pairs(),
                                                   self.dedupe_data.get_test_pairs()))
        clusters = self.deduper.cluster(pair_scores, threshold=0.5)
        input_data = self.dedupe_data.df_input_data.to_dict('index')
        clusters = self._add_singletons(input_data.keys(), clusters)
        clusters_eval = list(clusters)
        self._cleanup_scores(pair_scores)
        self.clusters = clusters_eval
        return self
    
    @staticmethod
    def training_accuracy(training_scores) -> float:
        training_data = pd.read_csv(project_root / DATA_PATH / "train.csv")

        y_true = training_data['label']
        y_pred = training_scores['score'].apply(lambda score: True if score > .5 else False)

        return accuracy_score(y_true, y_pred)

