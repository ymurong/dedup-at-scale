from fastapi import HTTPException, status

spark_execution_exception = HTTPException(
    status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="Problem in executing spark jobs!",
)

dedupe_missing_setting_exception = HTTPException(
    status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="Trained setting file not exists, need to train dedupe first!",
)