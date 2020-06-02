from typing import Union

from mason.clients.athena.athena_client import AthenaClient
from mason.clients.response import Response
from mason.clients.engines.execution import ExecutionClient
from mason.engines.execution.models.jobs import InvalidJob, ExecutedJob, Job


class AthenaExecutionClient(ExecutionClient):

    def __init__(self, config: dict):
        self.client = AthenaClient(config)

    def run_job(self, job: Job) -> Union[InvalidJob, ExecutedJob]:
        return self.client.run_job(job)

    def get_job(self, job_id: str, response: Response) -> Union[InvalidJob, ExecutedJob]:
        return self.client.get_job(job_id, response)

