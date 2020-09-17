from typing import Union, TypeVar

from returns.result import Result

S = TypeVar('S')
F = TypeVar('F')

def compute(result: Result[S, F]) -> Union[S,F]:
    return result._inner_value
