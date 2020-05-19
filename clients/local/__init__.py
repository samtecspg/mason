from typing import Tuple

from botocore.client import BaseClient

from clients.response import Response
from engines.execution.models.jobs import Job
from engines.execution.models.jobs.infer_job import InferJob

class LocalClient:

    def __init__(self, config: dict):
        self.threads = config.get("threads")

    def infer(self, job: InferJob):
        job.parameters

    def run_job(self, job: Job, response: Response) -> Tuple[Response, Job]:
        response.add_error("Job type not supported for local client")

    def client(self) -> BaseClient:
        pass

