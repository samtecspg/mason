
from abc import abstractmethod
from typing import List

class SchemaElement:
    @abstractmethod
    def __init__(self, attributes: dict):
        raise(NotImplementedError("Schema Element type not implemented"))

    @abstractmethod
    def to_dict(self) -> dict:
        raise(NotImplementedError("Schema Element type not implemented"))


class MetastoreSchema:

    @abstractmethod
    def __init__(self, schema):
        self._schema = schema
        self.type = ""
        self.children = []

    def __hash__(self):
        return 0

    def __eq__(self, other):
        # types are equal and there are no items outside of the intersection of their children
        symm_diff = set(self.children).symmetric_difference(set(other.children))
        return self.type == other.type and (len(symm_diff) == 0)

    def to_dict(self):
        return {
            'type': self.type,
            'children': list(map(lambda c: c.to_dict(), self.children))
        }

