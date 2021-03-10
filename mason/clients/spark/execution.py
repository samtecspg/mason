from typing import Union, Tuple, Optional

from mason.clients.engines.execution import ExecutionClient
from mason.clients.response import Response
from mason.clients.spark.spark_client import SparkClient
from mason.engines.execution.models.jobs import Job, ExecutedJob, InvalidJob

class SparkExecutionClient(ExecutionClient):

    def __init__(self, client: SparkClient):
        self.client: SparkClient = client 

    def run_job(self, job: Job, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        return self.client.run_job(job, response)

    def get_job(self, job_id: str, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        return self.client.get_job(job_id, response)


