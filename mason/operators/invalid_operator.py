from mason.clients.response import Response
from mason.operators.operator_response import OperatorResponse
from mason.util.environment import MasonEnvironment


class InvalidOperator:

    def __init__(self, reason: str):
        self.reason = reason

    def run(self, env: MasonEnvironment, response: Response, dry_run: bool = True, run_now: bool = False) -> OperatorResponse:
        response.add_error(f"Invalid Operator.  Reason:  {self.reason}")
        response.set_status(400)
        return OperatorResponse(response)
