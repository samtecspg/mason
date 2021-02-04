
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
    try:
        return l[i]
    except IndexError as e:
        return None

def dedupe(l: List[T]) -> List[T]:
    l.reverse() # generally want to take last
    l2 = list(set(l))
    l2.reverse()
    return l2


A = TypeVar("A")
B = TypeVar("B")
def sequence(l: List[Union[A, B]], type_a: Type[A], type_b: Type[B]) -> Tuple[List[A], List[B]]:
    l1: List[A] = []
    l2: List[B] = []
    for l0 in l:
        if isinstance(l0, type_a):
            l1.append(l0)
        else:
            assert(isinstance(l0, type_b))
            l2.append(l0)
    return l1, l2


C = TypeVar("C")
D = TypeVar("D")
def sequence_4(l: List[Union[A, B, C, D]], type_a: Type[A], type_b: Type[B], type_c: Type[C], type_d: Type[D]) -> Tuple[List[A], List[B], List[C], List[D]]:
    l1: List[A] = []
    l2: List[B] = []
    l3: List[C] = []
    l4: List[D] = []
    for l0 in l:
        if isinstance(l0, type_a):
            l1.append(l0)
        elif isinstance(l0, type_b):
            l2.append(l0)
        elif isinstance(l0, type_c):
            l3.append(l0)
        else:
            assert(isinstance(l0, type_d))
            l4.append(l0)
    return l1, l2, l3, l4

