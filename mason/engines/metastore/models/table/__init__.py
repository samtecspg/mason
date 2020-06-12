from datetime import datetime
from typing import Optional, List, Dict
import pandas as pd

from mason.clients.responsable import Responsable
from mason.clients.response import Response
from mason.engines.metastore.models.schemas.schema import Schema
from mason.engines.metastore.models.schemas.schema import SchemaConflict, InvalidSchema

class Table(Responsable):

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
    
    def to_response(self, response: Response):
        response.add_data(self.to_dict())
        return response

class InvalidTable(Responsable):
    def __init__(self, reason: str, invalid_schema: Optional[InvalidSchema] = None):
        self.invalid_schema = invalid_schema
        if invalid_schema:
            self.reason = reason + ":" + invalid_schema.reason
        else:
            self.reason = reason
            
    def to_response(self, response: Response):
        response.add_error(self.reason)
        return response

class TableNotFound(InvalidTable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_response(self, response: Response):
        response.add_error(self.reason)
        response.set_status(404)
        return response

class ConflictingTable(InvalidTable):
    def __init__(self, schema_conflict: SchemaConflict, *args, **kwargs):
        self.schema_conflict = schema_conflict
        super().__init__(*args, **kwargs)

    def to_response(self, response: Response):
        response.add_error(self.reason)
        response.set_status(403)
        return response

class InvalidTables(Responsable):

    def __init__(self, invalid_tables: List[InvalidTable]):
        self.invalid_tables = invalid_tables

    def conflicting_table(self):
        return next((x for x in self.invalid_tables if isinstance(x, ConflictingTable)), None)
    
    def message(self) -> str:
        return ", ".join(list(map(lambda i: i.reason, self.invalid_tables)))
        
    def to_response(self, response: Response):
        for it in self.invalid_tables:
            response = it.to_response(response)
            
        return response
            
        
# if isinstance(table, Table):
#     response.add_data(table.to_dict())
# else:
#     else:
#         tables = table
# 
#     for table in tables:
#         if table.schema_conflict:
#             response.add_data(table.schema_conflict.to_dict())
#         response.add_error(table.reason)
#         if "not found" in table.reason:
#             response.set_status(404)

