from src.dedupe_api.custom_dedupe import CustomDedupe
import duckdb
import logging.config
from src.resources.logging_conf import logging_config
import dedupe
from sklearn.linear_model import LogisticRegression
logging.config.dictConfig(logging_config)


def db():
    conn = duckdb.connect()
    return conn


if __name__ == '__main__':
    # default classifier is logistic regression
    custom_dedupe = CustomDedupe(db())
    custom_dedupe(classifier_name="LogisticRegression").scoring()