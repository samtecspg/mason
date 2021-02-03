from typing import Union, Tuple, Optional

from mason.clients.athena.athena_client import AthenaClient
from mason.clients.response import Response
from mason.clients.engines.execution import ExecutionClient
from mason.engines.execution.models.jobs import InvalidJob, ExecutedJob, Job

class AthenaExecutionClient(ExecutionClient):

    def __init__(self, client: AthenaClient):
        self.client = client

    def run_job(self, job: Job, response: Optional[Response] = None) -> Tuple[Union[InvalidJob, ExecutedJob], Response]:
        return self.client.run_job(job, response)

    def get_job(self, job_id: str, response: Optional[Response] = None) -> Tuple[Union[InvalidJob, ExecutedJob], Response]:
        return self.client.get_job(job_id, response)

