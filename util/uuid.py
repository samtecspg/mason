import re
import uuid

def uuid_regex():
    return re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")

def uuid4():
    return uuid.uuid4()