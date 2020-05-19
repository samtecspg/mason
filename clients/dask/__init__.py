from clients.dask.config import DaskConfig
from clients.dask.runner import EmptyDaskRunner
from clients.dask.runner.kubernetes import KubernetesWorker
from engines.execution.models.jobs import Job

from clients.response import Response

class DaskClient:
    def __init__(self, dask_config: dict):
        self.runner_type = dask_config.get("runner", {}).get("type", "")
        self.config = DaskConfig(dask_config)

    def client(self):
        return self.get_runner(self.runner_type)

    def run_job(self, job: Job, response: Response):
        job = self.client().run(self.config, job)
        response = job.running(response)
        return response

    def get_job(self, job_id: str, response: Response):
        raise NotImplementedError("Client not implemented")
        return response

    def get_runner(self, runner: str):
        if runner == "kubernetes":
            return KubernetesWorker()
        else:
            return EmptyDaskRunner()
