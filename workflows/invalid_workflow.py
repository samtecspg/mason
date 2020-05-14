from typing import Optional

from clients.response import Response
from util.environment import MasonEnvironment

class InvalidWorkflow:

    def __init__(self, reason: str):
        self.reason = reason

    def run(self, env: MasonEnvironment, response: Response, dry_run: bool = True, run_now: bool = False, schedule_name: Optional[str] = None) -> Response:
        response.add_error(f"Invalid Operator.  Reason:  {self.reason}")
        response.set_status(400)
        return response
