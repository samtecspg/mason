from datetime import datetime
from typing import Optional, List, Dict, Union
import pandas as pd

from mason.engines.metastore.models.schemas.text import TextSchema
from mason.clients.responsable import Responsable
from mason.clients.response import Response
from mason.engines.metastore.models.schemas.schema import Schema
from mason.engines.storage.models.path import Path

from dask.dataframe.core import DataFrame as DDataFrame
from pandas import DataFrame as PDataFrame

class Table(Responsable):

    def __init__(self, name: str, schema: Schema, created_at: Optional[datetime] = None, created_by: Optional[str] = None, database_name: Optional[str] = None, paths: List[Path] = [], source_path: Optional[Path] = None):
        self.name = name
        self.database_name = database_name
        self.schema = schema
        self.created_at = created_at
        self.created_by = created_by
        self.paths = paths
        self.source_path = source_path

        lt: Optional[str]
        if (isinstance(schema, TextSchema)):
            lt = schema.line_terminator
        else:
            lt = None

        self.line_terminator: Optional[str] = lt

    def as_df(self) -> PDataFrame:
        pd_dict: Dict[str, str] = self.schema.to_pd_dict()
        column_names: List[str] = list(pd_dict.keys())

        return pd.DataFrame(columns=column_names).astype(pd_dict)

    def as_dask_df(self) -> DDataFrame:
        import dask.dataframe as dd
        return dd.from_pandas(self.as_df(), npartitions=8)

    def to_dict(self) -> Dict[str, Union[str, datetime, dict]]:
        return {
            "Name": self.name,
            "CreatedAt": self.created_at or "",
            "CreatedBy": self.created_by or "",
            "Schema": self.schema.to_dict()
        }

    def to_response(self, response: Response):
        response.add_data(self.to_dict())
        return response

class TableList(Responsable):
    
    def __init__(self, tables: List[Table]):
        self.tables = tables
        
    def to_response(self, response: Response):
        data = {'Tables': list(map(lambda t: t.to_dict(), self.tables))}
        response.add_data(data)
        return response
            
