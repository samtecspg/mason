from typing import Any, List, Optional, Tuple

from mason.parameters.invalid_parameter import InvalidParameter
from mason.parameters.validated_parameter import ValidatedParameter
from mason.parameters.optional_parameter import OptionalParameter
from mason.util.logger import logger

class Parameter:

    def __init__(self, key: str, value: Any):
        self.key: str = key
        self.value = value

    def validate(self, required_keys: List[str], optional_keys: List[str]) -> Tuple[Optional[ValidatedParameter], Optional[OptionalParameter], Optional[InvalidParameter]]:
        required: Optional[ValidatedParameter] = None
        optional: Optional[OptionalParameter] = None
        invalid: Optional[InvalidParameter] = None

        if self.key in required_keys:
            required = ValidatedParameter(self.key, self.value)
        elif self.key in optional_keys:
            optional = OptionalParameter(self.key, self.value)
        else:
            logger.warning(f"Parameter is not specified as required or optional {self.key}")

        return required, optional, invalid

    def __hash__(self):
        return 0

    def __eq__(self, other):
        return self.key == other.key

    def to_dict(self) -> dict:
        return { self.key: self.value }

    def optional(self) -> 'OptionalParameter':
        return OptionalParameter(self.key, self.value)



