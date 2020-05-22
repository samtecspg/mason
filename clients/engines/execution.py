from typing import Union

from clients import Client
from clients.response import Response
from abc import abstractmethod

from engines.execution.models.jobs import InvalidJob, ExecutedJob, Job
from engines.metastore.models.credentials import MetastoreCredentials

class ExecutionClient(Client):

    ###  IMPORTANT:   This ensures that implemented specific execution client implementations conform to the needed template when 'mypy .' is run
    ###  which will return Cannot instantiate abstract class 'SparkExecutionClient' with abstract attribute 'run_job' (for example)

    @abstractmethod
    def run_job(self, job: Job) -> Union[InvalidJob, ExecutedJob]:
        return InvalidJob(job, "Client not implemented")

    @abstractmethod
    def get_job(self, job_id: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response

class EmptyExecutionClient(ExecutionClient):

    def run_job(self, job: Job) -> Union[InvalidJob, ExecutedJob]:
        return InvalidJob(job, "Client not implemented")

    def get_job(self, job_id: str, response: Response) -> Response:
        raise NotImplementedError("Client not implemented")
        return response
