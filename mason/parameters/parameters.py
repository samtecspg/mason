from typing import List

from mason.parameters.input_parameters import InputParameters
from mason.parameters.validated_parameters import ValidatedParameters

class Parameters:

    def __init__(self, parameter_def: dict):
        self.required_parameter_keys: List[str] = parameter_def.get("required", [])
        self.optional_parameter_keys: List[str] = parameter_def.get("optional", [])

    def validate(self, input_parameters: InputParameters) -> ValidatedParameters:
        return input_parameters.validate(self.required_parameter_keys, self.optional_parameter_keys)

    def to_dict(self):
        return {
            'required': self.required_parameter_keys,
            'optional': self.optional_parameter_keys
        }



