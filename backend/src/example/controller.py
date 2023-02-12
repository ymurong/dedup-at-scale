from fastapi import APIRouter, Depends
from . import service, schemas
from src.common.database_util import connection
from src.example.database import DATABASE_URL

example_app = APIRouter()


@example_app.get("/word_count", response_model=schemas.WordCountResponse,
                 description="compute word count results")
def word_count():
    with connection(DATABASE_URL) as conn:
        word_counts_results = service.compute_word_count(conn)
    response = {
        "word_counts": word_counts_results
    }
    return response
