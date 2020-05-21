from typing import Union

from util.uuid import uuid4

from clients.dask.runner import DaskRunner

from engines.execution.models.jobs import Job, InvalidJob, ExecutedJob
from engines.execution.models.jobs.infer_job import InferJob

from dask.distributed import Client
# import dask.array as da

class KubernetesWorker(DaskRunner):

    def __init__(self, config: dict):
        self.scheduler = config.get("scheduler")

    def run(self, job: Job) -> Union[ExecutedJob, InvalidJob]:
        if self.scheduler:
            # Warning: side-effects, client is used by dask implicitly
            client = Client(self.scheduler, asynchronous=True)

            if isinstance(job, InferJob):
                return self.infer(job)
            else:
                return InvalidJob("Job type not supported for Dask")
        else:
            return InvalidJob("Dask Scheduler not defined")


    def infer(self, job: InferJob) -> ExecutedJob:
        # array = da.ones((1000, 1000, 1000))
        # result = array.mean().compute()
        db = job.database
        path = job.path

        logs = []
        results = []

        logs.append(f"Infering schema for path {path.path_str} and storing at database {db.name}")

        id = str(uuid4())
        return ExecutedJob(job.type + "_" + id, results=results, logs=logs)




