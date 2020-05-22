from typing import Union

from engines.execution.models.jobs import Job, ExecutedJob, InvalidJob

from clients.engines.execution import ExecutionClient
from clients.spark import SparkClient
from clients.response import Response

class SparkExecutionClient(ExecutionClient):

    def __init__(self, config: dict):
        self.config = config
        self.client = SparkClient(config)

    def run_job(self, job: Job) -> Union[ExecutedJob, InvalidJob]:
        response = self.client.run_job(job)
        return response

    def get_job(self, job_id: str, response: Response) -> Response:
        response = self.client.get_job(job_id, response)

        return response

