from pydantic import BaseModel
from typing import List, Dict


class SourceData(BaseModel):
    database: str
    schema: str
    table_column_mapping: Dict[str, List[str]]


class JsonStructure(BaseModel):
    target_table_name: str
    target_lag: str
    warehouse: str
    source_data: list[SourceData]
