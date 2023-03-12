import logging
import pandas as pd
from duckdb import DuckDBPyConnection
from src.dedupe_api.custom_dedupe import CustomDedupe
from pathlib import Path
import dedupe
from sklearn.linear_model import LogisticRegression

logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent


def preprocessing(conn: DuckDBPyConnection, local=True, slicer=slice(None, None, None)) -> pd.DataFrame:
    return CustomDedupe(conn=conn, local_preprocessing=local).preprocessing(slicer=slicer).df_collection


def train_dedupe(conn: DuckDBPyConnection, reuse_setting=True, classifier=None) -> dedupe.StaticDedupe:
    return CustomDedupe(conn=conn, local_preprocessing=True).train(reuse_setting, classifier).deduper


def dedupe_scoring(conn: DuckDBPyConnection):
    custom_dedupe = CustomDedupe(conn=conn, local_preprocessing=True)
    custom_dedupe = custom_dedupe(classifier_name="LogisticRegression", reuse_setting=True).scoring()
    return custom_dedupe.df_train_scores, custom_dedupe.df_validation_scores, custom_dedupe.df_test_scores
