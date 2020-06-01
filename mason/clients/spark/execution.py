from typing import Union

from mason.clients.engines.execution import ExecutionClient
from mason.clients.response import Response
from mason.clients.spark import SparkClient
from mason.engines.execution.models.jobs import Job, ExecutedJob, InvalidJob

class SparkExecutionClient(ExecutionClient):

    def __init__(self, config: dict):
        self.config = config
        self.client = SparkClient(config)

    def run_job(self, job: Job) -> Union[ExecutedJob, InvalidJob]:
        response = self.client.run_job(job)
        return response

    def get_job(self, job_id: str, response: Response) -> Union[ExecutedJob, InvalidJob]:
        return self.client.get_job(job_id, response)


