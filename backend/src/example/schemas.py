from pydantic import BaseModel
from typing import Dict, List, Tuple


class WordCountResponse(BaseModel):
    word_counts: Dict[str, int]


class RecordPairsResponse(BaseModel):
    nb_original_pairs: int
    nb_block_pairs: int
    predicates: List
    record_pairs: List[Tuple[int, int]]
