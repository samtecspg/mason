
from typing import List, Any, Optional


def get(l: List[Any], i: int) -> Optional[Any]:
    return l[i] if i < len(l) else None

def flatten(l: List[Optional[Any]]) -> List[Any]:
    return [string for string in l if string != None]

def flatten_string(l: List[str]) -> List[str]:
    return [string for string in l if string != ""]

def flatten_array(l: List[List[str]]) -> List[str]:
    values = []
    for value in l:
        for subvalue in value:
            values.append(subvalue)
    return values

