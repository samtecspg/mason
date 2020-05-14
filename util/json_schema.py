import os
from typing import TypeVar, Optional, Type, Union, Tuple, List, Dict

from jsonschema import validate
from jsonschema.exceptions import SchemaError, ValidationError

from definitions import from_root
from util.exception import message

from util.json import parse_json

from util.yaml import parse_yaml

class InvalidSchemaDict:
    def __init__(self, d: dict, schema: dict, reason: str):
        self.dict = d
        self.schema = schema
        self.reason = reason

class ValidSchemaDict:
    def __init__(self, d: dict, schema: dict):
        self.dict = d
        self.schema = schema

T = TypeVar('T')
def object_from_json_schema(attr: dict, schema_file: str, cls: Type[T]) -> Union[T, InvalidSchemaDict] :

    ##  NOTE: Currently only works for json schema of depth 1
    ##  Make class init arguments handle nested arguments dicts
    ##  The dict will stil be validated to conform to the schema definition so you just need to ensure the nested class confirms to the json schema
    ##  TODO:  Remove need for this note ^

    schema = validate_schema(attr, schema_file)
    #  TODO: Fix this
    if "type" in attr:
        attr.pop("type")
    obj: Optional[T] = None

    if isinstance(schema, ValidSchemaDict):
        t: Optional[T]
        try:
            # TODO: Fix this
            return cls(**attr) #type: ignore
        except Exception as e:
            return InvalidSchemaDict(schema.dict, schema.schema, f"Object creation failed for {cls.__name__} with attributes {schema.dict}. {message(e)}")

    else:
        return schema

def parse_schemas(directory: str, type: str, cls: Type[T]) -> Tuple[List[T], List[str]]:
    objects: List[T] = []
    errors: List[str] = []

    for r, d, f in os.walk(directory):
        for file in f:
            if '.yaml' in file or '.yml' in file:
                file_path = os.path.join(r, file)
                config = parse_yaml(file_path)
                if config.get("type") == type:
                    schema = from_root(f"/{type}s/schema.json")
                    config["source_path"] = file_path
                    object = object_from_json_schema(config, schema, cls)

                    if isinstance(object, InvalidSchemaDict):
                        errors.append(object.reason)
                    else:
                        objects.append(object)

    return objects, errors

def validate_schema(d: Optional[dict], schema_file: str) -> Union[ValidSchemaDict, InvalidSchemaDict]:
    di = d or {}
    schema = {}
    if not di == {}:
        try:
            schema = parse_json(schema_file)
            validate(di, schema)
            return ValidSchemaDict(di, schema)
        except SchemaError as e:
            return InvalidSchemaDict(di, schema, f"\nSchema error {schema_file}: {e.message}")
        except ValidationError as e:
            return InvalidSchemaDict(di, schema, f"\nSchema error {schema_file}: {e.message}")
        except FileNotFoundError as e:
            return InvalidSchemaDict(di, schema, f"\nSchema not found: {e.filename}")
    else:
        return ValidSchemaDict(di, {})


A = TypeVar("A")
B = TypeVar("B")

def sequence(l: List[Union[A, B]], type_a: Type[A], type_b: Type[B]) -> Tuple[List[A], List[B]]:
    l1: List[A] = []
    l2: List[B] = []
    for l0 in l:
        if isinstance(l0, type_a):
            l1.append(l0)
        else:
            assert(isinstance(l0, type_b))
            l2.append(l0)
    return l1, l2

