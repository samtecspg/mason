from typing import List

from mason.definitions import from_root
from mason.util.json_schema import object_from_json_schema, InvalidSchemaDict
from mason.util.logger import logger

class BadTest:
    def __init__(self, bad: str):
        self.bad = bad

class BasicTest:
    def __init__(self, test: str):
        self.test = test

class NestedTest:
    def __init__(self, test2: str, test3: List[str]):
        self.test2 = test2
        self.test3 = test3

class ComplexTest:
    def __init__(self, test: List[NestedTest]):
        self.test =  test

BASIC_SCHEMA = from_root("/test/support/schemas/basic_schema.json")
COMPLEX_SCHEMA = from_root("/test/support/schemas/complex_schema.json")

class TestJsonSchema:

    def test_basic_json_schema(self):
        obj = object_from_json_schema({"test": "test", "type": "test"}, BASIC_SCHEMA, BasicTest)
        if isinstance(obj, BasicTest):
            assert(obj.test == "test")
            #  NOTE THAT REMOVING type: ignore throws an error in mypy here which is what we want.
            #  Don't know how to test this so including this note
            try:
                obj.other #type: ignore
            except AttributeError as e:
                assert(str(e) == '\'BasicTest\' object has no attribute \'other\'')

    def test_basic_json_schema_invalid(self):
        logger.set_level("fatal")
        obj = object_from_json_schema({"test": "test", "type": "test"}, BASIC_SCHEMA, BadTest)
        if isinstance(obj, InvalidSchemaDict):
            assert(obj.__class__.__name__ == "InvalidSchemaDict")
            assert(obj.reason[0:27] == "Object creation failed for ")

