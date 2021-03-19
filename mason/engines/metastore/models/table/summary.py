from typing import Optional, List, Tuple

from dask.dataframe import DataFrame as DDataFrame
from pandas import DataFrame

from mason.clients.responsable import Responsable
from mason.clients.response import Response
from mason.engines.metastore.models.table.table import Table
from mason.util.exception import message

class BaseSummary():
    
    def __init__(self, name: str, non_null: int, max, min, distinct_count: int):
        self.name = name
        self.non_null = non_null
        self.max = max
        self.min = min
        self.distinct_count = distinct_count
    
    def to_dict(self) -> dict:
        return {
            "non_null": self.non_null,
            "max": self.max,
            "min": self.min,
            "distinct_count": self.distinct_count
        }

class TableSummary(Responsable):
    def __init__(self, summaries: List[BaseSummary]):
        self.summaries = summaries
        
    def to_response(self, response: Response) -> Response:
        response.add_data(self.to_dict())
        return response
    
    def to_dict(self) -> dict:
        return {
            "Summaries": {s.name: s.to_dict() for s in self.summaries}
        }

def from_ddf(table: Table, ddf: DDataFrame, response: Response) -> Tuple[TableSummary, Response]:

    # TODO: Combine into a single query
    non_null = execute_query(ddf, non_null_query(table), response)
    max = execute_query(ddf, max_query(table), response)
    min = execute_query(ddf, min_query(table), response)
    distinct_count = execute_query(ddf, distinct_count_query(table), response)
    # average = execute_query(average_query(table), response, c) 
    
    summaries = list(map(lambda col: BaseSummary(col, safe_get_value(non_null, col), safe_get_value(max, col), safe_get_value(min, col), safe_get_value(distinct_count, col)), table.column_names()))
    
    return TableSummary(summaries), response

def safe_get_value(df: Optional[DataFrame], col: str):
    if isinstance(df, DataFrame):
        dict = df.to_dict()
        value = dict.get(col)
        if value:
            return value.get(0)
    
def execute_query(ddf: DDataFrame, query: str, response: Response) -> Optional[DataFrame]:
    try:
        from dask_sql import Context
        c = Context()
        c.create_table("df", ddf)
        return c.sql(query).compute()
    except Exception as e:
        response.add_error(f"Error executing SQL query: {message(e)}")
        return None

def for_each_column_query(table: Table, statement: str) -> str:
    query = "SELECT "
    for col in table.column_names():
        query += statement.replace("$col", col.strip(" ")) + ", "
    query = query.rstrip(", ")
    query += " FROM df"
    return query

def non_null_query(table: Table) -> str:
    return for_each_column_query(table, f"(SUM(CASE WHEN $col IS NULL THEN 0 ELSE 1 END)) as $col")

def max_query(table: Table) -> str:
    return for_each_column_query(table, "(MAX($col)) as $col")

def min_query(table: Table) -> str:
    return for_each_column_query(table, "(MIN($col)) as $col")

def distinct_count_query(table: Table) -> str:
    return for_each_column_query(table, "(COUNT(DISTINCT($col))) as $col")

def average_query(table: Table) -> str:
    return for_each_column_query(table, "(AVERAGE($col)) as $col")
