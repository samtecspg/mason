from typing import Union

from clients.dask import DaskClient
from engines.execution.models.jobs import Job, ExecutedJob, InvalidJob

from clients.engines.execution import ExecutionClient
from clients.response import Response

class DaskExecutionClient(ExecutionClient):

    def __init__(self, config: dict):
        self.config = config
        self.client = DaskClient(config)

    def run_job(self, job: Job) -> Union[ExecutedJob, InvalidJob]:
        return self.client.run_job(job)

    def get_job(self, job_id: str, response: Response) -> Union[ExecutedJob, InvalidJob]:
         return self.client.get_job(job_id, response)
