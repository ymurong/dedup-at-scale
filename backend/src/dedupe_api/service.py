import pandas as pd
from duckdb import DuckDBPyConnection
from src.common.spark_util import create_spark_session
from typing import Dict, Tuple, TextIO, List
from src.resources.conf import SPARK_MASTER, DATA_PATH, DEDUPE_SETTING_PATH, BLOCKING_FIELDS
from src.dedupe_api.exception import spark_execution_exception
from src.common.dedupe_util import DedupeData, Dataset
from src.common.preprocessing import lower_case, year
from pathlib import Path
import dedupe
import logging
import io

logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent


def preprocessing(conn: DuckDBPyConnection) -> pd.DataFrame:
    # ## load data
    dataset = Dataset(conn=conn, data_path=DATA_PATH)
    df_collection: pd.DataFrame = dataset.get_collection()[:4]

    # create spark session
    try:
        with create_spark_session("dedupe_preprocessing", spark_master=SPARK_MASTER) as spark:
            # spark computation
            dfs_collection = spark.createDataFrame(df_collection)
            df_collection = dfs_collection.transform(lower_case).transform(year).toPandas()
    except Exception as e:
        logger.error(e, exc_info=True)
        raise spark_execution_exception
    return df_collection


def train_predicates(conn: DuckDBPyConnection, reuse_setting=True) -> List[str]:
    """
    Train blocking rules, save it to setting file for later use, and return the trained predicates
    :param reuse_setting:
    :param conn:
    :return: predicates
    """
    # if reuse setting then we will load the existing setting file and prevent from training again
    settings_file = project_root / DEDUPE_SETTING_PATH
    if reuse_setting:
        if settings_file.is_file():
            with open(settings_file, 'rb') as f:
                deduper = dedupe.StaticDedupe(f)
                predicates = [str(predicate) for predicate in deduper.predicates]
                return predicates

    # retrain the model, regenerate setting file and return the predicates
    # ## load data
    dedupe_data = DedupeData(db=conn, data_path=DATA_PATH)
    input_data: dict = dedupe_data.input_data
    training_data: str = dedupe_data.training_data.json()

    # ## train dedupe

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(BLOCKING_FIELDS)

    with io.StringIO(training_data) as f_training_data:
        deduper.prepare_training(input_data, f_training_data)

    # Using the examples we just labeled, train the deduper and learn
    # blocking predicates
    logger.info("dedupe start training predicates... ")
    deduper.train()

    # Save our setting file to disk
    logger.info("writing dedupe setting file to disk ... ")
    with open(settings_file, 'wb') as sf:
        deduper.write_settings(sf)

    predicates = [str(predicate) for predicate in deduper.predicates]
    return predicates
