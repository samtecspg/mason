from typing import List, Optional

from mason.parameters.operator_parameters import OperatorParameters
from mason.parameters.validated_parameters import ValidatedParameters

class InputParameters:

    def __init__(self, required: Optional[List[str]] = None, optional: Optional[List[str]] = None):
        self.required: List[str] = required or []
        self.optional: List[str] = optional or []

    def validate(self, input_parameters: OperatorParameters) -> ValidatedParameters:
        return input_parameters.validate(self.required, self.optional)

    def to_dict(self):
        return {
            'required': self.required,
            'optional': self.optional
        }



