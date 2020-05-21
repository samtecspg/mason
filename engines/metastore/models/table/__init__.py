from datetime import datetime
from typing import Optional
from engines.metastore.models.schemas import Schema

class Table:

    def __init__(self, name: str, schema: Schema, created_at: Optional[datetime], created_by: Optional[str]):
        self.name = name
        self.schema = schema
        self.created_at = created_at
        self.created_by = created_by

class InvalidTable:

    def __init__(self, reason: str):
        self.reason = reason