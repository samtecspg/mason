from typing import Union

from clients import Client
from clients.response import Response
from abc import abstractmethod

from engines.execution.models.jobs import InvalidJob, ExecutedJob, Job

class ExecutionClient(Client):

    @abstractmethod
    def run_job(self, job: Job) -> Union[InvalidJob, ExecutedJob]:
        return InvalidJob(job, "Client not implemented")

    @abstractmethod
    def get_job(self, job_id: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

