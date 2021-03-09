from typing import Union, Tuple, Optional

from abc import abstractmethod

from mason.clients.base import Client
from mason.clients.response import Response
from mason.engines.execution.models.jobs import InvalidJob, ExecutedJob, Job

class ExecutionClient:
    
    @abstractmethod
    def __init__(self, client: Client):
        self.client = client
    
    @abstractmethod
    def run_job(self, job: Job, response: Optional[Response] = None) -> Tuple[Union[InvalidJob, ExecutedJob], Response]:
        raise NotImplementedError("Client not implemented")

    @abstractmethod
    def get_job(self, job_id: str, response: Optional[Response] = None) -> Tuple[Union[InvalidJob, ExecutedJob], Response]:
        raise NotImplementedError("Client not implemented")

