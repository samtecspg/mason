from typing import List

from mason.clients.responsable import Responsable
from mason.clients.response import Response
from mason.engines.metastore.models.table.table import TableList


class Database(Responsable):

    def __init__(self, name: str, tables: TableList):
        self.name = name
        self.tables = tables
        
    def to_response(self, response: Response):
        return self.tables.to_response(response)

class InvalidDatabase(Responsable):

    def __init__(self, reason: str):
        self.reason = reason

    def to_response(self, response: Response):
        response.add_error(f"InvalidDatabase: {self.reason}")
        return response


class DatabaseList(Responsable):
    
    def __init__(self, databases: List[Database], invalid_databases: List[InvalidDatabase]):
        self.databases = databases
        self.invalid_databases = invalid_databases
        
    def to_response(self, response: Response = Response()):
        for invalid in self.invalid_databases:
            response = invalid.to_response(response)

        if len(self.databases) > 0:
            data = {'Databases': list(map(lambda t: t.to_response(response), self.databases))}
            response.add_data(data)
        else:
            response.add_error("No databases found for configured metastore")
            
        return response