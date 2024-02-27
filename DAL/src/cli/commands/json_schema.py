from pydantic import BaseModel


class JsonStructure(BaseModel):
    target_table_name: str
    target_lag: str
    warehouse: str
    source_sql: str
