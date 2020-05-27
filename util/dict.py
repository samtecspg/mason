from functools import reduce
from typing import Dict, List

def merge(dicts: List[Dict]) -> Dict:
    def merge_with(d1: Dict, d2: Dict) -> Dict:
        res = {**d1, **d2}
        return res
    return reduce(merge_with, dicts, {})