from clients.engines.execution import ExecutionClient
from clients.spark import SparkClient
from clients.response import Response
from engines.metastore.models.credentials import MetastoreCredentials

class SparkExecutionClient(ExecutionClient):

    def __init__(self, config: dict):
        self.config = config
        self.client = SparkClient(config)

    def run_job(self, job_type: str, metastore_credentials: MetastoreCredentials, parameters: dict, response: Response) -> Response:
        response = self.client.run_job(job_type, metastore_credentials, parameters, response)
        return response

    def get_job(self, job_id: str, response: Response) -> Response:
        response = self.client.get_job(job_id, response)

        return response

