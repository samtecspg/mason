from typing import Optional, Tuple, Union

from mason.clients.local.local_client import LocalClient
from mason.clients.response import Response
from mason.clients.engines.execution import ExecutionClient
from mason.engines.execution.models.jobs import InvalidJob, ExecutedJob, Job

class LocalExecutionClient(ExecutionClient):
    
    def __init__(self, client: LocalClient):
        self.client = client 
        
    def is_async(self) -> bool:
        return False
    
    def run_job(self, job: Job, response: Optional[Response] = None) -> Tuple[Union[InvalidJob, ExecutedJob], Response]:
        raise NotImplementedError("Client run_job not implemented")

    def get_job(self, job_id: str, response: Optional[Response] = None) -> Tuple[Union[InvalidJob, ExecutedJob], Response]:
        raise NotImplementedError("Client not implemented")

