def to_class_case(s: str) -> str:
    return "".join(list(map(lambda c: c.capitalize(), s.split("_"))))
