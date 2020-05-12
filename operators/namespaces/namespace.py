from typing import List

from operators.operator import Operator


class Namespace:
    def __init__(self, namespace: str, ops: List[Operator]):
        self.namespace = namespace
        self.operators = ops

    def to_dict(self):
        return list(map(lambda o: o.to_dict(), self.operators))

    def to_dict_brief(self):
        return { self.namespace: list(map(lambda o: o.command, self.operators)) }




