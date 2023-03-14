from src.dedupe_api.custom_dedupe import CustomDedupe
import duckdb
import logging.config
from src.resources.logging_conf import logging_config
import dedupe
from sklearn.linear_model import LogisticRegression
from sklearn import svm 
from sklearn.calibration import CalibratedClassifierCV 

logging.config.dictConfig(logging_config)


def db():
    conn = duckdb.connect()
    return conn


if __name__ == '__main__':
    # default classifier is logistic regression
    estimator = LogisticRegression()
    #estimator = CalibratedClassifierCV(svm.LinearSVC())
    # retrain it by setting reuse_setting to False
    deduper = CustomDedupe(db()).train(reuse_setting=False, classifier=estimator).deduper
    assert type(deduper) == dedupe.StaticDedupe
