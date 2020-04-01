
from clients.engines.execution import ExecutionClient
from clients.spark import SparkClient
from clients.response import Response

class SparkExecutionClient(ExecutionClient):

    def __init__(self, config: dict):
        self.client = SparkClient(config)

    def run_job(self, job_name: str, response: Response):
        response = self.client.run_job(job_name, response)
        return response

