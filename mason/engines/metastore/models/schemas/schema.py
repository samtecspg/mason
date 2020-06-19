
from typing import Sequence, Set, List, Dict
from abc import ABCMeta

from mason.util.dict import merge

class SchemaElement:

    def __init__(self, name: str, type: str):
        self.name = name
        self.type = type

    def to_dict(self) -> dict:
        return {
            "Name": self.name,
            "Type": self.type
        }

    def to_pd_dict(self) -> dict:
        return {self.name: self.lookup_pd_type(self.type)}

    def lookup_pd_type(self, type: str) -> str:
        return "object"

class Schema(object):

    __metaclass__ = ABCMeta

    def __init__(self, columns: Sequence[SchemaElement], type: str):
        self.type = type
        self.columns: Sequence[SchemaElement] = columns

    def __hash__(self):
        return 0

    def __eq__(self, other):
        diff = self.diff(other)
        return (len(diff) == 0)

    def diff(self, other: 'Schema') -> Set[SchemaElement]:
        if not self.type == other.type:
            return set(self.columns)
        else:
            return set(self.columns).symmetric_difference(set(other.columns))

    def to_dict(self) -> dict:
        return {
            'SchemaType': self.type,
            'Columns': list(map(lambda c: c.to_dict(), self.columns))
        }

    def to_pd_dict(self) -> Dict:
        return merge(list(map(lambda c: c.to_pd_dict(), self.columns)))

class EmptySchema(Schema):

    def __init__(self):
        self.type = ""
        self.columns: Sequence[SchemaElement] = []

    def __eq__(self, other):
        return isinstance(other, EmptySchema)

    def __hash__(self):
        return 0

    def to_dict(self):
        return {}

class InvalidSchema:

    def __init__(self, reason: str):
        self.reason = reason


class InvalidSchemaElement:

    def __init__(self, reason: str):
        self.reason = reason


class SchemaConflict:

    def __init__(self, unique_schemas: List[Schema], schema_diff: Schema):
        self.unique_schemas = unique_schemas
        self.schema_diff = schema_diff

    def to_dict(self):
        unique_schema_dicts: List[dict] = list(map(lambda s: s.to_dict(), self.unique_schemas))

        return {
            'SchemaConflicts': {
                'CountDistinctSchemas': len(unique_schema_dicts),
                'DistinctSchemas': unique_schema_dicts,
                'NonOverlappingColumns': list(map(lambda s: {'name': s.name, 'type': s.type}, self.schema_diff.columns))
            }
        }

def emptySchema():
    return EmptySchema()
