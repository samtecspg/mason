
from jsonschema import validate
from jsonschema.exceptions import SchemaError, ValidationError
from util.json import parse_json
from util.logger import logger

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

