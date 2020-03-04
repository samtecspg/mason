
from jsonschema import validate # type: ignore
from jsonschema.exceptions import SchemaError, ValidationError # type: ignore
from util.json import parse_json, print_json

def validate_schema(d: dict, schema_file: str) -> bool:
    try:
        schema = parse_json(schema_file)
        validate(d, schema)
        return True
    except SchemaError as e:
        print(f"Schema error: {e.message}")
        return False
    except ValidationError as e:
        print(f"Schema error: {e.message}")
        return False
    except FileNotFoundError as e:
        print(f"Schema not found: {e.filename}")
        return False

