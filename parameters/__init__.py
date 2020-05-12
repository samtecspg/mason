from typing import Optional, List, Tuple, Union

from parameters.invalid_parameter import InvalidParameter
from parameters.parameter import Parameter, ValidatedParameter, OptionalParameter
from util.list import dedupe, get
import re

class InputParameters:
    def __init__(self, parameter_string: Optional[str] = None, parameter_path: Optional[str] = None):
        self.parameter_string = parameter_string
        self.parameter_path = parameter_path

        if parameter_string:
            parameters, invalid = self.parse_string(parameter_string)
        elif parameter_path:
            parameters, invalid = self.parse_path(parameter_path)

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

    def parse_path(self, param_path: str):
        return []

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
        return (" \n").join(list(map(lambda i: i.reason, self.invalid_parameters)))


