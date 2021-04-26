from typing import Optional, List

from mason.engines.metastore.models.schemas.schema import SchemaConflict, InvalidSchema

from mason.clients.response import Response

from mason.clients.responsable import Responsable
from mason.engines.storage.models.path import Path


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

    def __init__(self, invalid_tables: List[InvalidTable], error: Optional[str] = None):
        self.invalid_tables = invalid_tables
        self.error = error

    def conflicting_table(self):
        return next((x for x in self.invalid_tables if isinstance(x, ConflictingTable)), None)

    def message(self) -> str:
        return ", ".join(list(map(lambda i: i.reason, self.invalid_tables)))

    def to_response(self, response: Response) -> Response:
        for it in self.invalid_tables:
            response = it.to_response(response)

        if self.error:
            response.add_error(self.error)

        return response

