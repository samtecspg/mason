
from abc import abstractmethod
from clients.spark.config import SparkConfig
from engines.execution.models.jobs import Job
from engines.metastore.models.credentials import MetastoreCredentials

class SparkRunner:

    @abstractmethod
    def run(self, config: SparkConfig, job_name: str, metastore_credentials: MetastoreCredentials, params: dict) -> Job:

        raise NotImplementedError("Runner not implemented")

class EmptySparkRunner(SparkRunner):

    def run(self, config: SparkConfig, job_name: str, metastore_credentials: MetastoreCredentials, params: dict) -> Job:
            raise NotImplementedError("Runner not implemented")


