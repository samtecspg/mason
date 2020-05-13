
from typing import List, Optional, TypeVar, Union, Tuple, Type

T = TypeVar("T")
R = TypeVar("R")

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

def dedupe(l: List[T]) -> List[T]:
    l.reverse() # generally want to take last
    l2 = list(set(l))
    l2.reverse()
    return l2

def split_type(l : List[Union[T, R]]) -> Tuple[List[T], List[R]]:
    l1: List[T] = []
    l2: List[R] = []
    for l0 in l:
        if isinstance(l0, Type[T]):
            l1.append(l0)
        else:
            l2.append(l0)
    return l1, l2
