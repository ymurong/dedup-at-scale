from fastapi import HTTPException, status
from typing import Any, Dict, Optional

spark_execution_exception = HTTPException(
    status.HTTP_500_INTERNAL_SERVER_ERROR,
    detail="Problem in executing spark jobs!",
)