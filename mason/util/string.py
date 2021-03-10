import re

def to_class_case(s: str) -> str:
    return "".join(list(map(lambda c: c.capitalize(), s.split("_"))))


def to_snake_case(s: str) -> str:
    return re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()
