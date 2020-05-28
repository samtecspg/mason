from typing import Union

from engines.execution.models.jobs import Job, InvalidJob, ExecutedJob

from clients.athena import AthenaClient
from clients.engines.execution import ExecutionClient
from clients.response import Response

class AthenaExecutionClient(ExecutionClient):

    def __init__(self, config: dict):
        self.client = AthenaClient(config)

    def run_job(self, job: Job) -> Union[InvalidJob, ExecutedJob]:
        return self.client.run_job(job)

    def get_job(self, job_id: str, response: Response) -> Union[InvalidJob, ExecutedJob]:
        return self.client.get_job(job_id, response)

