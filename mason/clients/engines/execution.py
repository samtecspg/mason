from typing import Union, Tuple, Optional

from abc import abstractmethod

from mason.clients.base import Client
from mason.clients.response import Response
from mason.engines.execution.models.jobs import InvalidJob, ExecutedJob, Job

class ExecutionClient:
    
    def is_async(self) -> bool:
        return True
    
    @abstractmethod
    def __init__(self, client: Client):
        self.client = client
    
    @abstractmethod
    def run_job(self, job: Job, response: Response = Response()) -> Tuple[Union[InvalidJob, ExecutedJob], Response]:
        raise NotImplementedError("Client not implemented")

    @abstractmethod
    def get_job(self, job_id: str, response: Response = Response()) -> Tuple[Union[InvalidJob, ExecutedJob], Response]:
        raise NotImplementedError("Client not implemented")

