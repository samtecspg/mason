from clients.athena import AthenaClient
from clients.engines.execution import ExecutionClient
from clients.response import Response
from engines.metastore.models.credentials import MetastoreCredentials

class AthenaExecutionClient(ExecutionClient):

    def __init__(self, config: dict):
        self.client = AthenaClient(config)

    def run_job(self, job_type: str, metastore_credentials: MetastoreCredentials, parameters: dict, response: Response) -> Response:
        response, job = self.client.run_job(job_type, metastore_credentials, parameters, response)
        if job:
            response = job.add_data(response)
        return response

    def get_job(self, job_id: str, response: Response) -> Response:
        response, job = self.client.get_job(job_id, response)
        response = job.add_data(response)
        return response
