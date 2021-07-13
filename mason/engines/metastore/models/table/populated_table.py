from pandas import DataFrame
from dask.dataframe import DataFrame as DDataFrame
from mason.clients.responsable import Responsable
import dask.dataframe as dd
from mason.engines.metastore.models.table.table import Table

class PopulatedTable(Responsable):
    
    def __init__(self, table: 'Table', dataframe: DataFrame):
        self.table = table
        self.df: DataFrame = dataframe

    def ddf(self) -> DDataFrame:
        return dd.from_pandas(self.df, 1)

