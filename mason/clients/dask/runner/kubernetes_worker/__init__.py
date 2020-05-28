from typing import Union
from dask.distributed import Client

from mason.clients.dask.runner import DaskRunner
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob, Job


class KubernetesWorker(DaskRunner):

    def __init__(self, config: dict):
        self.scheduler = config.get("scheduler")

    #  No valid Dask implementations until v 1.05
    def run(self, job: Job) -> Union[ExecutedJob, InvalidJob]:
        if self.scheduler:
            # Warning: side-effects, client is used by dask implicitly
            client = Client(self.scheduler, asynchronous=True)
            return InvalidJob(job, "Job type not supported for Dask")
        else:
            return InvalidJob(job, "Dask Scheduler not defined")


