from typing import List, Dict, Optional
from operators.operator import Operator
from operators.namespaces.namespace import Namespace

from util.list import flatten_array

def from_ops(operators: List[Operator]) -> List[Namespace]:
    namespaces: Dict[str, List[Operator]] = {}

    for operator in operators:
        ops = namespaces.get(operator.namespace) or []
        ops.append(operator)
        namespaces[operator.namespace] = ops

    return list(map(lambda s: Namespace(s[0], s[1]), namespaces.items()))


def filter(namespaces: List[Namespace], namespace: Optional[str] = None) -> List[Namespace]:
    if namespace:
        return [n for n in namespaces if n.namespace == namespace]
    else:
        return namespaces


def get_all(namespaces: List[Namespace]) -> List[Operator]:
    return flatten_array(list(map(lambda n: n.operators, namespaces)))


def get(namespaces: List[Namespace], namespace: str, command: str) -> Optional[Operator]:
    ops = get_all(namespaces)
    return next((x for x in ops if x.namespace == namespace and x.command == command), None)



