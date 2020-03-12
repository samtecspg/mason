
from util.logger import logger

def dedupe(d: dict) -> dict:
    result: dict = {}
    i = list(d.items())
    i.reverse()
    for key,value in i:
        if key not in result.keys():
            result[key] = value
    return result