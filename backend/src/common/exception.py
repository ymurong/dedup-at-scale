from fastapi import HTTPException, status

missing_clean_collection_data_exception = HTTPException(
    status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="Cleaned collection data is missing! You can either activate local preprocessing"
           "or manually create the cleaned collection data under resources/data directory",
)
