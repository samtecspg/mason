from typing import Union

from mason.clients.dask import DaskClient
from mason.clients.engines.execution import ExecutionClient
from mason.clients.response import Response
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job


class DaskExecutionClient(ExecutionClient):

    def __init__(self, config: dict):
        self.config = config
        self.client = DaskClient(config)

    def run_job(self, job: Job) -> Union[ExecutedJob, InvalidJob]:
        return self.client.run_job(job)

    def get_job(self, job_id: str, response: Response) -> Union[ExecutedJob, InvalidJob]:
         return self.client.get_job(job_id, response)
