
from abc import abstractmethod

from clients.dask import DaskConfig
from clients.spark.config import SparkConfig
from engines.execution.models.jobs import Job
from engines.metastore.models.credentials import MetastoreCredentials

class DaskRunner:

    @abstractmethod
    def run(self, config: DaskConfig, job_name: str, metastore_credentials: MetastoreCredentials, params: dict) -> Job:

        raise NotImplementedError("Runner not implemented")

class EmptyDaskRunner(DaskRunner):

    def run(self, config: SparkConfig, job_name: str, metastore_credentials: MetastoreCredentials, params: dict) -> Job:
            raise NotImplementedError("Runner not implemented")


