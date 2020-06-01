from typing import List

from mason.engines.metastore.models.table import Table

class Database:

    def __init__(self, name: str, tables: List[Table]):
        self.name = name
        self.tables = tables

class InvalidDatabase:

    def __init__(self, reason: str):
        self.reason = reason
