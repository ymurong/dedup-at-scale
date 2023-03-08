from src.dedupe_api.service import preprocessing, train_predicates
import duckdb
import logging.config
from src.resources.logging_conf import logging_config

logging.config.dictConfig(logging_config)


def db():
    conn = duckdb.connect()
    return conn


if __name__ == '__main__':
    predicates = train_predicates(db(), reuse_setting=True)
    assert type(predicates) == list
