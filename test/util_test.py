from typing import List

from definitions import from_root
from util.json_schema import object_from_json_schema, InvalidSchemaDict
from util.logger import logger

class TestBad:
    def __init__(self, bad: str):
        self.bad = bad

class TestBasic:
    def __init__(self, test: str):
        self.test = test

class TestNested:
    def __init__(self, test2: str, test3: List[str]):
        self.test2 = test2
        self.test3 = test3

class TestComplex:
    def __init__(self, test: List[TestNested]):
        self.test =  test

BASIC_SCHEMA = from_root("/test/support/schemas/basic_schema.json")
COMPLEX_SCHEMA = from_root("/test/support/schemas/complex_schema.json")

class TestJsonSchema:

    def test_basic_json_schema(self):
        obj = object_from_json_schema({"test": "test", "type": "test"}, BASIC_SCHEMA, TestBasic)
        if isinstance(obj, TestBasic):
            assert(obj.test == "test")
            #  NOTE THAT REMOVING type: ignore throws an error in mypy here which is what we want.
            #  Don't know how to test this so including this note
            try:
                obj.other #type: ignore
            except AttributeError as e:
                assert(str(e) == '\'TestBasic\' object has no attribute \'other\'')

    def test_basic_json_schema_invalid(self):
        logger.set_level("fatal")
        obj = object_from_json_schema({"test": "test", "type": "test"}, BASIC_SCHEMA, TestBad)
        if isinstance(obj, InvalidSchemaDict):
            assert(obj.__class__.__name__ == "InvalidSchemaDict")
            assert(obj.reason[0:27] == "Object creation failed for ")

