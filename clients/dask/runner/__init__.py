
from abc import abstractmethod

from clients.dask import DaskConfig
from engines.execution.models.jobs import Job

class DaskRunner:

    @abstractmethod
    def run(self, config: DaskConfig, job: Job) -> Job:
        raise NotImplementedError("Runner not implemented")
        return job

class EmptyDaskRunner(DaskRunner):

    def run(self, config: DaskConfig, job: Job) -> Job:
        raise NotImplementedError("Runner not implemented")
        return job


