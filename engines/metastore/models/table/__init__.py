from datetime import datetime
from typing import Optional, List, Dict
from engines.metastore.models.schemas import Schema
from engines.metastore.models.schemas.schema import SchemaConflict, InvalidSchema
import pandas as pd

from util.logger import logger


class Table:

    def __init__(self, name: str, schema: Schema, created_at: Optional[datetime] = None, created_by: Optional[str] = None):
        self.name = name
        self.schema = schema
        self.created_at = created_at
        self.created_by = created_by

    def as_df(self):
        pd_dict: Dict[str, str] = self.schema.to_pd_dict()
        column_names: List[str] = list(pd_dict.keys())

        return pd.DataFrame(columns=column_names).astype(pd_dict)

class InvalidTable:

    def __init__(self, reason: str, schema_conflict: Optional[SchemaConflict] = None, invalid_schema: Optional[InvalidSchema] = None):
        self.reason = reason
        self.schema_conflict =  schema_conflict
        self.invalid_schema = invalid_schema