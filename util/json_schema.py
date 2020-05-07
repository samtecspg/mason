import os
from typing import TypeVar, Optional, Type, Union, Tuple, List

from jsonschema import validate
from jsonschema.exceptions import SchemaError, ValidationError

from definitions import from_root
from util.exception import message

from util.json import parse_json
from util.logger import logger
import inspect

from util.yaml import parse_yaml


def validate_schema(d: dict, schema_file: str) -> Union[str, bool]:
    validation: Union[str, bool] = True
    if d and not d == {}:
        try:
            schema = parse_json(schema_file)
            validate(d, schema)
        except SchemaError as e:
            validation = f"\nSchema error {schema_file}: {e.message}"
        except ValidationError as e:
            validation = f"\nSchema error {schema_file}: {e.message}"
        except FileNotFoundError as e:
            validation = f"\nSchema not found: {e.filename}"
    else:
        validation = False
    return validation


T = TypeVar('T')
def from_json_schema(attr: dict, schema_file: str, cls: Type[T]) -> Tuple[Optional[T], Optional[str]] :

    ##  NOTE: Currently only works for json schema of depth 1
    ##  Make class init arguments handle nested arguments dicts
    ##  The dict will stil be validated to conform to the schema definition so you just need to ensure the nested class confirms to the json schema
    ##  TODO:  Remove need for this note ^

    validation = validate_schema(attr, schema_file)
    attr.pop("type")
    error: Optional[str] = None
    obj: Optional[T] = None
    if isinstance(validation, bool) and validation == True:
        t: Optional[T]
        try:
            # TODO: Fix this
            obj = cls(**attr) #type: ignore
            params = [x for x in list(inspect.signature(cls.__init__).parameters.keys()) if x != 'self']
        except Exception as e:
            error = message(e)
            obj = None
    else:
        error = f"Schema Validation failed for schema: {schema_file}.  Reason: {validation}"

    return obj, error

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
                    object, error = from_json_schema(config, schema, cls)

                    if object:
                        objects.append(object)
                    if error:
                        errors.append(error)

    return objects, errors
