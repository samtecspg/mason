from typing import List, Optional

from mason.parameters.invalid_parameter import InvalidParameter
from mason.parameters.optional_parameter import OptionalParameter
from mason.parameters.validated_parameter import ValidatedParameter
from mason.parameters.parameter import Parameter

class ValidatedParameters:

    def __init__(self, parsed_parameters: List[Parameter], validated_parameters: List[ValidatedParameter], optional_parameters: List[OptionalParameter], invalid_parameters: List[InvalidParameter]):
        self.parsed_parameters: List[Parameter] = parsed_parameters
        self.validated_parameters: List[ValidatedParameter] = validated_parameters
        self.optional_parameters: List[OptionalParameter] = optional_parameters
        self.invalid_parameters: List[InvalidParameter] = invalid_parameters

    def get(self, params, attribute) -> Optional[str]:
        return next((x.value for x in params if x.key == attribute), None)

    def get_required(self, attribute: str) -> str:
        return self.get(self.validated_parameters, attribute) or ""

    def get_optional(self, attribute: str) -> Optional[str]:
        return self.get(self.optional_parameters, attribute)

    def get_parsed(self, attribute: str) -> Optional[str]:
        return self.get(self.parsed_parameters, attribute)

    def has_invalid(self) -> bool:
        return len(self.invalid_parameters) > 0

    def messages(self) -> str:
        return (", ").join(list(map(lambda i: i.reason, self.invalid_parameters)))
