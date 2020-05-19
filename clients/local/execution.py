from typing import Tuple

from engines.execution.models.jobs import Job

from clients.engines.execution import ExecutionClient
from clients.local import LocalClient

from clients.response import Response

class LocalExecutionClient(ExecutionClient):

    def __init__(self, config: dict):
        self.client = LocalClient(config)

    def run_job(self, job: Job, response: Response) -> Tuple[Response, Job]:
        raise NotImplementedError("Client not implemented")
        return response

    def get_job(self, job_id: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

