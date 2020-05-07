
from typing import List, Optional, TypeVar

T = TypeVar("T")

def flatten(l: List[Optional[T]]) -> List[T]:
    return [string for string in l if string]

def flatten_array(l: List[List[T]]) -> List[T]:
    values = []
    for value in l:
        for subvalue in value:
            values.append(subvalue)
    return values

def get(l: List[T], i: int) -> Optional[T]:
    return l[i] if i < len(l) else None
