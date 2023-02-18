from fastapi import APIRouter, Depends
from . import service, schemas
from src.example.database import DATABASE_URL
from src.common.database_util import DBConnector
from duckdb import DuckDBPyConnection

example_app = APIRouter()


def get_conn():
    conn = DBConnector(DATABASE_URL).cursor()
    try:
        yield conn
    finally:
        conn.close()


@example_app.get("/word_count", response_model=schemas.WordCountResponse,
                 description="compute word count results")
def word_count(conn: DuckDBPyConnection = Depends(get_conn)):
    word_counts_results = service.compute_word_count(conn)
    response = {
        "word_counts": word_counts_results
    }
    return response
