from fastapi import HTTPException, status

spark_execution_exception = HTTPException(
    status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="Problem in executing spark jobs!",
)

dedupe_missing_setting_exception = HTTPException(
    status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="Trained setting file not exists, need to train dedupe first!",
)

dedupe_missing_train_score_results_exception = HTTPException(
    status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="Train score results file not exists, need to score train data first!",
)