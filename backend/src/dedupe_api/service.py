import logging
import pandas as pd
from duckdb import DuckDBPyConnection
from src.common.spark_util import create_spark_session
from typing import Dict, Tuple, TextIO, List
from src.resources.conf import SPARK_MASTER, DATA_PATH, DEDUPE_SETTING_PATH
from src.dedupe_api.exception import spark_execution_exception
from src.common.dedupe_util import DedupeData, Dataset
from src.common.spark_preprocessing import lower_case, abs_year, inversed_pauthor_ptitle, null_type_fix
from pathlib import Path
from src.common.local_preprocessing import pauthor_to_set
import dedupe

logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent


def preprocessing(conn: DuckDBPyConnection, local=True, slicer=slice(None, None, None)) -> pd.DataFrame:
    """
    if local is True, then DedupeData class will preprocess it locally,
    otherwise unpreprocessed data will be fed to spark
    """
    # ## load data
    dedupe_data = DedupeData(db=conn, data_path=DATA_PATH, local=local)
    df_collection: pd.DataFrame = dedupe_data.df_input_data[slicer]

    if local:
        return df_collection

    # create spark session
    try:
        with create_spark_session("dedupe_preprocessing", spark_master=SPARK_MASTER) as spark:
            # spark computation
            sdf_collection = spark.createDataFrame(df_collection).repartition(8, "pid")
            sdf_collection = inversed_pauthor_ptitle(spark, sdf_collection)
            sdf_collection = sdf_collection.transform(lower_case) \
                .transform(abs_year)
            sdf_collection2 = sdf_collection.transform(null_type_fix)
            # parquet will write to one of the worker
            sdf_collection2.coalesce(1).write.mode("overwrite").parquet("file:///tmp/collection_parquet")
    except Exception as e:
        logger.error(e, exc_info=True)
        raise spark_execution_exception
    return df_collection


def train_dedupe(conn: DuckDBPyConnection, reuse_setting=True, classifier=None) -> dedupe.StaticDedupe:
    """
    Train blocking rules and classifier, save it to setting file for later use, and return the trained predicates
    :param conn: duckDBConnection
    :param reuse_setting: If True then the existing setting (classifier and predicates) will be used
    :param classifier: sklearn classifier instance
    :return: trained dedupe.StaticDedupe instance
    """
    # if reuse setting then we will load the existing setting file and prevent from training again
    settings_file = project_root / DEDUPE_SETTING_PATH
    if reuse_setting:
        if settings_file.is_file():
            with open(settings_file, 'rb') as f:
                return dedupe.StaticDedupe(f)

    # retrain the model, regenerate setting file and return the predicates
    # ## load data
    dedupe_data = DedupeData(db=conn, data_path=DATA_PATH)
    input_data: dict = dedupe_data.df_input_data.transform(pauthor_to_set).to_dict('index')
    training_file = project_root / "resources/data/training_data.json"

    # ## train dedupe

    def pauthors(input_data):
        for record in input_data.values():
            yield record['pauthor']

    BLOCKING_FIELDS = [
        {'field': 'pauthor', 'type': 'Set', 'corpus': pauthors(input_data)},
        {'field': 'ptitle', 'type': 'String'},
        # {'field': 'pyear', 'type': 'Exact', 'has missing': True},
        # {'field': 'pjournal', 'type': 'String', 'has missing': True},
        # {'field': 'pbooktitle', 'type': 'String', 'has missing': True},
        # {'field': 'ptype', 'type': 'String', 'has missing': True}
    ]
    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(BLOCKING_FIELDS)

    # be default, the classifier is a L2 regularized logistic regression classifier.
    if classifier is not None:
        deduper.classifier = classifier

    with open(training_file, "r") as f_training_data:
        deduper.prepare_training(input_data, f_training_data, sample_size=10000, blocked_proportion=.9)

    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    # print('starting active labeling...')
    # dedupe.console_label(deduper)

    # Using the examples we just labeled, train the deduper and learn

    # blocking predicates
    logger.info("dedupe start training predicates... ")
    deduper.train()

    # When finished, save our training away to disk
    with open(training_file, 'w') as tf:
        deduper.write_training(tf)

    # Save our setting file to disk
    logger.info("writing dedupe setting file to disk ... ")
    with open(settings_file, 'wb') as sf:
        deduper.write_settings(sf)

    # load static dedupe and return
    with open(settings_file, 'rb') as f:
        return dedupe.StaticDedupe(f)
