from typing import Optional

from mason.clients.response import Response
from mason.resources.invalid import InvalidResource
from mason.util.environment import MasonEnvironment

class InvalidWorkflow(InvalidResource):

    def __init__(self, reason: str):
        super().__init__(reason)

    def execute(self, env: MasonEnvironment, response: Response, dry_run: bool = True, run_now: bool = False, schedule_name: Optional[str] = None) -> Response:
        response.add_error(f"Invalid Operator.  Reason:  {self.reason}")
        response.set_status(400)
        return response
