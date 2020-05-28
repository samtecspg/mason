
import datetime
import json
from mason.util.logger import logger

def parse_json(file_path: str):
    with open(file_path) as f:
      return json.load(f)

def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

def to_json(d: dict):
    dump = json.dumps(
        d,
        sort_keys=False,
        indent=1,
        default=default
    )
    return dump

def print_json(d: dict):
    formatted_json = to_json(d)
    # colorful_json = highlight(formatted_json, lexers.JsonLexer(), formatters.TerminalFormatter())
    colorful_json = formatted_json
    return logger.error(colorful_json)

def print_json_1level(d: dict):
    out = {}
    for key, value in d.items():
        out[key] = str(value)
    print_json(out)
