from datetime import datetime
from typing import Optional, List, Dict
import pandas as pd

from mason.clients.responsable import Responsable
from mason.clients.response import Response
from mason.engines.metastore.models.schemas.schema import Schema
from mason.engines.metastore.models.schemas.schema import SchemaConflict, InvalidSchema

from dask.dataframe.core import DataFrame as DDataFrame
from pandas import DataFrame as PDataFrame

from mason.engines.storage.models.path import Path


class Table(Responsable):

    def __init__(self, name: str, schema: Schema, created_at: Optional[datetime] = None, created_by: Optional[str] = None, database_name: Optional[str] = None, paths: List[Path] = []):
        self.name = name
        self.schema = schema
        self.created_at = created_at
        self.created_by = created_by
        self.paths = paths

    def as_df(self) -> PDataFrame:
        pd_dict: Dict[str, str] = self.schema.to_pd_dict()
        column_names: List[str] = list(pd_dict.keys())

        return pd.DataFrame(columns=column_names).astype(pd_dict)

    def as_dask_df(self) -> DDataFrame:
        import dask.dataframe as dd
        return dd.from_pandas(self.as_df(), npartitions=8)

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
        response.add_data(self.schema_conflict.to_dict())
        response.set_status(403)
        return response

class InvalidTables(Responsable):

    def __init__(self, invalid_tables: List[InvalidTable]):
        self.invalid_tables = invalid_tables

    def conflicting_table(self):
        return next((x for x in self.invalid_tables if isinstance(x, ConflictingTable)), None)
    
    def message(self) -> str:
        return ", ".join(list(map(lambda i: i.reason, self.invalid_tables)))
        
    def to_response(self, response: Response) -> Response:
        for it in self.invalid_tables:
            response = it.to_response(response)
            
        return response
            
