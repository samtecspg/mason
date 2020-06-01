from typing import Union

from abc import abstractmethod

from mason.clients.client import Client
from mason.clients.response import Response
from mason.engines.execution.models.jobs import InvalidJob, ExecutedJob, Job


class ExecutionClient(Client):

    @abstractmethod
    def run_job(self, job: Job) -> Union[InvalidJob, ExecutedJob]:
        return InvalidJob(job, "Client not implemented")

    @abstractmethod
    def get_job(self, job_id: str, response: Response) -> Union[InvalidJob, ExecutedJob]:
        raise NotImplementedError("Client not implemented")
        return response

