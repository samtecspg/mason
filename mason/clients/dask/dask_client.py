from typing import Union, Tuple, Optional

from mason.clients.base import Client
from mason.clients.dask.runner.dask_runner import EmptyDaskRunner
from mason.clients.dask.runner.kubernetes_worker.kubernetes_worker import KubernetesWorker
from mason.clients.response import Response
from mason.engines.execution.models.jobs import Job, ExecutedJob, InvalidJob

class DaskClient(Client):
    def __init__(self, runner: dict):
        self.runner_config = runner 
        self.runner_type = runner.get("type") or ""
        
    def client(self):
        return self.get_runner(self.runner_type, self.runner_config)

    def run_job(self, job: Job, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        return self.client().run(job, response)

    def get_job(self, job_id: str, response: Optional[Response] = None) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        raise NotImplementedError("Client not implemented")

    def get_runner(self, runner: str, config: dict):
        if runner == "kubernetes_worker":
            return KubernetesWorker(config)
        else:
            return EmptyDaskRunner()
        
