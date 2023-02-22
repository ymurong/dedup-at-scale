from pydantic import BaseModel
from typing import Dict


class WordCountResponse(BaseModel):
    word_counts: Dict[str, int]

class RecordPairsResponse(BaseModel):
    record_pairs: Dict[str, str]
