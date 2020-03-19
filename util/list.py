
from typing import List, Any, Optional


def get(l: List[Any], i: int) -> Optional[Any]:
    return l[i] if i < len(l) else None


def flatten(l: List[Optional[Any]]) -> List[Any]:
    return list(filter(lambda x: not x == None, l))