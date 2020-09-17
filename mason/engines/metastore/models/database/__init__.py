from mason.engines.metastore.models.table import TableList

class Database:

    def __init__(self, name: str, tables: TableList):
        self.name = name
        self.tables = tables

class InvalidDatabase:

    def __init__(self, reason: str):
        self.reason = reason
