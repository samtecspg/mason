from typing import List, Dict, Any

from mason.parameters.invalid_parameter import InvalidParameter
from mason.parameters.parameter import Parameter
from mason.util.json_schema import object_from_json_schema
from mason.util.exception import message

def parse_dict(d: dict, schema: str):
    valid: List[Parameter] = []
    invalid: List[InvalidParameter] = []
    validated = object_from_json_schema(d, schema, dict)
    if isinstance(validated, dict):  # can now be confident it is a one level dict
        v: Dict[str, Any] = validated
        for key, value in v.items():
            try:
                valid.append(Parameter(key, value))
            except Exception as e:
                invalid.append(InvalidParameter(f"Invalid Parameter: {message(e)}"))

    else:
        invalid.append(InvalidParameter(
            "Parameters do not conform to specified schema in parameters/schema.json.  Must be of form key:value"))

    return valid, invalid

