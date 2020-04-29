
from typing import Sequence, Set, Dict


class SchemaElement:
    def __init__(self, name: str, type: str):
        self.name = name
        self.type = type

    def to_dict(self) -> dict:
        return {
            "Name": self.name,
            "Type": self.type
        }


class MetastoreSchema:

    def __init__(self, columns: Sequence[SchemaElement], type: str):
        self.type = type
        self.columns: Sequence[SchemaElement] = columns

    def __hash__(self):
        return 0

    def __eq__(self, other):
        diff = self.diff(other)
        return (len(diff) == 0)

    def diff(self, other: 'MetastoreSchema') -> Set[SchemaElement]:
        if not self.type == other.type:
            return set(self.columns)
        else:
            return set(self.columns).symmetric_difference(set(other.columns))

    def to_dict(self):
        return {
            'SchemaType': self.type,
            'Columns': list(map(lambda c: c.to_dict(), self.columns))
        }

def emptySchema():
    return MetastoreSchema([], "")
