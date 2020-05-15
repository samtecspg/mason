from typing import Optional, List, Tuple, Dict, Any

from util.environment import MasonEnvironment
from util.session import get_session_config

from configurations.configurations import get_current_config

from definitions import from_root
from parameters.invalid_parameter import InvalidParameter
from parameters.parameter import Parameter, ValidatedParameter, OptionalParameter
from util.json_schema import object_from_json_schema
from util.list import dedupe, get
import re
from util.exception import message

from util.yaml import parse_yaml_invalid

class WorkflowParameter:
    def __init__(self, step: str, config_id: str, parameters: 'InputParameters'):
        self.config_id = config_id
        self.step = step
        self.parameters = parameters

class WorkflowParameters:
    def __init__(self, parameter_path: Optional[str] = None, parameter_dict: Optional[dict] = None):
        parameters: List[WorkflowParameter] = []
        if parameter_path:
            parameters, invalid = self.parse_path(parameter_path or "")
        elif parameter_dict:
            parameters, invalid = self.parse_param_dict(parameter_dict or {})
        else:
            invalid = [InvalidParameter("No parameter path or json specified")]

        self.parameters: List[WorkflowParameter] = parameters
        self.invalid: List[InvalidParameter] = invalid

    def parse_param_dict(self, param_dict: dict) -> Tuple[List[WorkflowParameter], List[InvalidParameter]]:
        valid: List[WorkflowParameter] = []
        invalid: List[InvalidParameter] = []
        if isinstance(param_dict, dict):
            validated = object_from_json_schema(param_dict, from_root("/parameters/workflow_schema.json"), dict)
            if isinstance(validated, dict):  #can now be confident it is matches schema definition
                for key, value in validated.items():
                    config_id: str = str(value["config_id"])
                    parameters: Dict[str,Dict[str, Any]] = value["parameters"]
                    valid_step, invalid_step = parse_dict(parameters, from_root("/parameters/schema.json"))
                    ip = InputParameters()
                    ip.parameters = valid_step
                    ip.invalid = invalid_step
                    valid.append(WorkflowParameter(key, config_id, ip))
            else:
                invalid.append(InvalidParameter(f"Parameters do not conform to specified schema in parameters/workflow_schema.json.  Must be of form step_id: key:value.  {validated.reason}"))

        return valid, invalid


    def parse_path(self, param_path: str) -> Tuple[List[WorkflowParameter], List[InvalidParameter]]:
        parsed = parse_yaml_invalid(param_path)
        valid: List[WorkflowParameter] = []
        invalid: List[InvalidParameter] = []
        if isinstance(parsed, dict):
            valid, invalid = self.parse_param_dict(parsed)
        else:
            invalid.append(InvalidParameter(f"Invalid parameter yaml: {parsed}"))

        return valid, invalid

class InputParameters:
    def __init__(self, parameter_string: Optional[str] = None, parameter_path: Optional[str] = None):
        self.parameter_string = parameter_string
        self.parameter_path = parameter_path

        if parameter_string:
            parameters, invalid = self.parse_string(parameter_string)
        elif parameter_path:
            parameters, invalid = self.parse_path(parameter_path)
        else:
            parameters, invalid = ([], [])

        self.parameters: List[Parameter] = dedupe(parameters)
        self.invalid: List[InvalidParameter] = invalid

    def to_dict(self) -> List[dict]:
        return list(map(lambda p: p.to_dict(), self.parameters))

    def unsafe_get(self, attr: str) -> Optional[str]:
        op = get([p for p in self.parameters if p.key == attr], 0)
        if op:
            return op.value
        else:
            return None

    def parse_path(self, param_path: str) -> Tuple[List[Parameter], List[InvalidParameter]]:
        parsed = parse_yaml_invalid(param_path)
        valid: List[Parameter] = []
        invalid: List[InvalidParameter] = []
        if isinstance(parsed, dict):
            valid, invalid = parse_dict(parsed, from_root("/parameters/schema.json"))
        else:
            invalid.append(InvalidParameter(parsed))

        return valid, invalid

    def parse_string(self, param_string: str) -> Tuple[List[Parameter], List[InvalidParameter]]:
        pattern = r"([^,^:]+:[^,^:]+)"
        pattern_guide = "<param1>:<value1>,<param2>:<value2>"
        matches = re.findall(pattern, param_string)
        parameters: List[Parameter] = []
        invalid: List[InvalidParameter] = []

        if len(matches) > 0:
            for m in matches:
                s = m.split(":")
                a: Optional[Parameter]
                if len(s) == 2:
                    parameters.append(Parameter(s[0], s[1]))
        else:
            invalid.append(InvalidParameter(f"Warning:  Parameter string does not conform to needed pattern: {pattern_guide}"))

        return parameters, invalid

    def validate(self, required_keys: List[str], optional_keys: List[str]) -> 'ValidatedParameters':
        parsed_parameters = self.parameters
        validated_parameters: List[ValidatedParameter] = []
        optional_parameters: List[OptionalParameter] = []
        invalid_parameters: List[InvalidParameter] = self.invalid

        for p in self.parameters:
            valid, optional, invalid = p.validate(required_keys, optional_keys)
            if valid:
                validated_parameters.append(valid)
            elif optional:
                optional_parameters.append(optional)
            elif invalid:
                invalid_parameters.append(invalid)

        validated_keys: List[str] = list(map(lambda v: v.key ,validated_parameters))

        for k in (set(required_keys).difference(validated_keys)):
            invalid_parameters.append(InvalidParameter(f"Required parameter not specified: {k}"))

        return ValidatedParameters(parsed_parameters, validated_parameters, optional_parameters, invalid_parameters)

    def to_string(self):
        self.parameter_string or self.parameter_path or ""


class Parameters:

    def __init__(self, parameter_def: dict):
        self.required_parameter_keys: List[str] = parameter_def.get("required", [])
        self.optional_parameter_keys: List[str] = parameter_def.get("optional", [])

    def validate(self, input_parameters: 'InputParameters') -> 'ValidatedParameters':
        return input_parameters.validate(self.required_parameter_keys, self.optional_parameter_keys)

    def to_dict(self):
        return {
            'required': self.required_parameter_keys,
            'optional': self.optional_parameter_keys
        }

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


class ValidatedParameters():

    def __init__(self, parsed_parameters: List[Parameter], validated_parameters: List[ValidatedParameter], optional_parameters: List[OptionalParameter], invalid_parameters: List[InvalidParameter]):
        self.parsed_parameters: List[Parameter] = parsed_parameters
        self.validated_parameters: List[ValidatedParameter] = validated_parameters
        self.optional_parameters: List[OptionalParameter] = optional_parameters
        self.invalid_parameters: List[InvalidParameter] = invalid_parameters

    def get(self, params, attribute) -> Optional[str]:
        return next((x.value for x in params if x.key == attribute), None)

    def get_required(self, attribute: str) -> str:
        return self.get(self.validated_parameters, attribute) or ""

    def get_optional(self, attribute: str) -> str:
        return self.get(self.optional_parameters, attribute) or ""

    def get_parsed(self, attribute: str) -> Optional[str]:
        return self.get(self.parsed_parameters, attribute)

    def has_invalid(self) -> bool:
        return len(self.invalid_parameters) > 0

    def messages(self) -> str:
        return (", ").join(list(map(lambda i: i.reason, self.invalid_parameters)))


