from src.dedupe_api.service import preprocessing, train_dedupe
import duckdb
import logging.config
from src.resources.logging_conf import logging_config
import dedupe
logging.config.dictConfig(logging_config)


def db():
    conn = duckdb.connect()
    return conn


if __name__ == '__main__':
    deduper = train_dedupe(db(), reuse_setting=False)
    assert type(deduper) == dedupe.StaticDedupe
