
from abc import abstractmethod
from clients.spark.config import SparkConfig
from clients.response import Response
from engines.metastore.models.credentials import MetastoreCredentials

class SparkRunner:

    @abstractmethod
    def run(self, config: SparkConfig, job_name: str, metastore_credentials: MetastoreCredentials, params: dict, response: Response):

        raise NotImplementedError("Runner not implemented")

class EmptySparkRunner(SparkRunner):

    def run(self, config: SparkConfig, job_name: str, metastore_credentials: MetastoreCredentials, params: dict, response: Response):
            raise NotImplementedError("Runner not implemented")


