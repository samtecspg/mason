import re
from typing import Optional, List, Tuple

from mason.definitions import from_root
from mason.parameters.invalid_parameter import InvalidParameter
from mason.parameters.optional_parameter import OptionalParameter
from mason.parameters.parameters import Parameters
from mason.parameters.util import parse_dict as parse_dict
from mason.parameters.validated_parameter import ValidatedParameter
from mason.parameters.validated_parameters import ValidatedParameters
from mason.parameters.parameter import Parameter
from mason.util.list import dedupe, get
from mason.util.yaml import parse_yaml_invalid

class OperatorParameters(Parameters):
    def __init__(self, parameter_string: Optional[str] = None, parameter_path: Optional[str] = None, parameter_dict: Optional[dict] = None):
        self.parameter_string = parameter_string
        self.parameter_path = parameter_path

        if parameter_string:
            parameters, invalid = self.parse_string(parameter_string)
        elif parameter_path:
            parameters, invalid = self.parse_path(parameter_path)
        elif parameter_dict:     
            parameters, invalid = parse_dict(parameter_dict, from_root("/parameters/schema.json"))
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

    def validate(self, required_keys: List[str], optional_keys: List[str]) -> ValidatedParameters:
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
        return self.parameter_string or self.parameter_path or ""

