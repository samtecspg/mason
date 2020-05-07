from typing import List

from definitions import from_root
from util.json_schema import from_json_schema
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
        test, error = from_json_schema({"test": "test", "type": "test"}, BASIC_SCHEMA, TestBasic)
        if test:
            assert(test.test == "test")
            #  NOTE THAT REMOVING type: ignore throws an error in mypy here which is what we want.
            #  Don't know how to test this so including this note
            try:
                test.other #type: ignore
            except AttributeError as e:
                assert(str(e) == '\'TestBasic\' object has no attribute \'other\'')

    def test_basic_json_schema_invalid(self):
        logger.set_level("fatal")
        test, error = from_json_schema({"test": "test", "type": "test"}, BASIC_SCHEMA, TestBad)
        assert(test == None)
        assert(len(error or "") > 0)

