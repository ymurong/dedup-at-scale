from pydantic import BaseModel as PydanticBaseModel
from typing import List, Mapping, Any
import orjson

RecordDict = Mapping[str, Any]


class RecordDictPair(tuple[RecordDict, RecordDict]):
    pass


def record_dict_pair_json_encoder(record_tuple):
    return {
        "__class__": "tuple",
        "__value__": [record_tuple[0], record_tuple[1]]
    }


class BaseModel(PydanticBaseModel):
    """
    All the instances of BaseModel should serialize
    those types the same way
    """

    class Config:
        @classmethod
        def orjson_dumps(cls, v, *, default):
            # orjson.dumps returns bytes, to match standard json.dumps we need to decode
            return orjson.dumps(v, default=default).decode()

        arbitrary_types_allowed = True
        json_dumps = orjson_dumps
        json_loads = orjson.loads
        json_encoders = {
            RecordDictPair: record_dict_pair_json_encoder
        }


class TrainingData(BaseModel):
    match: List[RecordDictPair]  # list of entity pairs
    distinct: List[RecordDictPair]  # list of entity pairs
