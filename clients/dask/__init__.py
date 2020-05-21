from clients.dask.config import DaskConfig
from clients.dask.runner import EmptyDaskRunner
from clients.dask.runner.kubernetes_worker import KubernetesWorker
from engines.execution.models.jobs import Job, ExecutedJob

from clients.response import Response


class DaskClient:
    def __init__(self, dask_config: dict):
        self.runner_config = dask_config.get("runner", {})
        self.runner_type = self.runner_config.get("type")

    def client(self):
        return self.get_runner(self.runner_type, self.runner_config)

    def run_job(self, job: Job, response: Response):
        job = self.client().run(job)
        if isinstance(job, ExecutedJob):
            job.to_response(response)
        else:
            response.add_error(job.reason)

        return response

    def get_job(self, job_id: str, response: Response):
        raise NotImplementedError("Client not implemented")
        return response

    def get_runner(self, runner: str, config: dict):
        if runner == "kubernetes_worker":
            return KubernetesWorker(config)
        else:
            return EmptyDaskRunner()
