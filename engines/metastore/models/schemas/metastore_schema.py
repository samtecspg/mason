
from abc import abstractmethod
from typing import Sequence


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
        self.columns = columns

    def __hash__(self):
        return 0

    def __eq__(self, other):
        # types are equal and there are no items outside of the intersection of their children
        symm_diff = set(self.columns).symmetric_difference(set(other.columns))
        return self.type == other.type and (len(symm_diff) == 0)

    def to_dict(self):
        return {
            'SchemaType': self.type,
            'Columns': list(map(lambda c: c.to_dict(), self.columns))
        }

def emptySchema():
    return MetastoreSchema([], "")
