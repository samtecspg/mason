from engines.execution.models.jobs import Job

from clients.aws.athena import AthenaClient
from clients.engines.execution import ExecutionClient
from clients.response import Response

class AthenaExecutionClient(ExecutionClient):

    def __init__(self, config: dict):
        self.client = AthenaClient(config)

    def run_job(self, job: Job, response: Response) -> Response:
        response, job = self.client.run_job(job, response)
        if job:
            response = job.add_data(response)
        return response

    def get_job(self, job_id: str, response: Response) -> Response:
        response, job = self.client.get_job(job_id, response)
        response = job.add_data(response)
        return response

