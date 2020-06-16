from typing import Optional

from mason.operators.operator_response import OperatorResponse


class InvalidDagStep:

    def __init__(self, reason: str, response: Optional[OperatorResponse] = None):
        self.reason = reason
        self.operator_response = response