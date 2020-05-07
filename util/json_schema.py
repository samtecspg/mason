from typing import TypeVar, Optional, Type

from jsonschema import validate
from jsonschema.exceptions import SchemaError, ValidationError
from util.json import parse_json
from util.logger import logger
import inspect

def validate_schema(d: dict, schema_file: str) -> bool:
    valid = False
    if d:
        if not d == {}:
            try:
                schema = parse_json(schema_file)
                validate(d, schema)
                valid = True
            except SchemaError as e:
                logger.error(f"\nSchema error {schema_file}: {e.message}")
            except ValidationError as e:
                logger.error(f"\nSchema error {schema_file}: {e.message}")
            except FileNotFoundError as e:
                logger.error(f"\nSchema not found: {e.filename}")
        else:
            valid = False
    return valid


T = TypeVar('T')
def from_json_schema(attr: dict, schema_file: str, cls: Type[T]) -> Optional[T]:

    ##  NOTE: Currently only works for json schema of depth 1

    valid = validate_schema(attr, schema_file)
    obj: Optional[T] = None
    if valid:
        matches = True
        t: Optional[T]
        try:
            # TODO: Fix this
            obj = cls(**attr) #type: ignore
            params = [x for x in list(inspect.signature(cls.__init__).parameters.keys()) if x != 'self']
        except Exception as e:
            logger.error(e)
            obj = None

    return obj
