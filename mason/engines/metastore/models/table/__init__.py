from datetime import datetime
from typing import Optional, List, Dict
import pandas as pd

from mason.engines.metastore.models.schemas.schema import Schema
from mason.engines.metastore.models.schemas.schema import SchemaConflict, InvalidSchema

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

    def to_dict(self):
        return {
            "Name": self.name,
            "CreatedAt": self.created_at or "",
            "CreatedBy": self.created_by or "",
            "Schema": self.schema.to_dict()
        }


class InvalidTable:

    def __init__(self, reason: str, schema_conflict: Optional[SchemaConflict] = None, invalid_schema: Optional[InvalidSchema] = None):
        self.schema_conflict =  schema_conflict
        self.invalid_schema = invalid_schema
        if invalid_schema:
            self.reason = reason + ":" + invalid_schema.reason
        else:
            self.reason = reason

