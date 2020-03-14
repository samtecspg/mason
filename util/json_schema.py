
from jsonschema import validate # type: ignore
from jsonschema.exceptions import SchemaError, ValidationError # type: ignore
from util.json import parse_json
from util.logger import logger

def validate_schema(d: dict, schema_file: str) -> bool:
    valid = False
    if not d == {}:
        try:
            schema = parse_json(schema_file)
            validate(d, schema)
            valid = True
        except SchemaError as e:
            logger.error(f"\nSchema error: {e.message}")
        except ValidationError as e:
            logger.error(f"\nSchema error: {e.message}")
        except FileNotFoundError as e:
            logger.error(f"\nSchema not found: {e.filename}")

    else:
        valid = False
    return valid

