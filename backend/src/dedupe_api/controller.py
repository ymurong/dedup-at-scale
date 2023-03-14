from fastapi import APIRouter, Depends, Query
from . import service, schemas
from src.dedupe_api.database import DATABASE_URL
from src.common.database_util import DBConnector
from duckdb import DuckDBPyConnection
from typing import Optional

dedupe_app = APIRouter()


def get_conn():
    conn = DBConnector(DATABASE_URL).cursor()
    try:
        yield conn
    finally:
        conn.close()


@dedupe_app.get("/predicates", response_model=schemas.PredicatesResponse,
                description="fit dedupe model with labeled triplets and return the blocking rules (predicates)")
def predicates(
        conn: DuckDBPyConnection = Depends(get_conn),
        reuse_setting: Optional[bool] = Query(True,
                                              description="Pass True if you want to reuse previous trained setting.")
):
    predicate_list = service.train_predicates(conn, reuse_setting=reuse_setting)
    response = {
        "predicates": predicate_list
    }
    return response


@dedupe_app.get("/train_accuracy", response_model=float,
                description="gets the accuracy of the training")
def train_accuracy(
    conn: DuckDBPyConnection = Depends(get_conn)
):
    return service.train_accuracy(conn=conn)