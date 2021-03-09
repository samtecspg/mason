from typing import Optional, Tuple, Union

from mason.clients.local.local_client import LocalClient
from mason.clients.response import Response
from mason.clients.engines.execution import ExecutionClient
from mason.engines.execution.models.jobs import InvalidJob, ExecutedJob, Job
from mason.engines.execution.models.jobs.infer_job import InferJob
from mason.engines.execution.models.jobs.summary_job import SummaryJob

class LocalExecutionClient(ExecutionClient):
    
    def __init__(self, client: LocalClient):
        self.client = client 

    def run_job(self, job: Job, response: Optional[Response] = None) -> Tuple[Union[InvalidJob, ExecutedJob], Response]:
        resp: Response = response or Response()
        if isinstance(job, InferJob):
            final, response = job.run(resp)
        elif isinstance(job, SummaryJob):
            final, response = job.run(resp)
        else:
            final = InvalidJob(f"Job type {job.type} not supported")
            
        return final, resp

    def get_job(self, job_id: str, response: Optional[Response] = None) -> Tuple[Union[InvalidJob, ExecutedJob], Response]:
        raise NotImplementedError("Client not implemented")

