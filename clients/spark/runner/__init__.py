
from abc import abstractmethod
from typing import Union

from clients.spark.config import SparkConfig
from engines.execution.models.jobs import Job, ExecutedJob, InvalidJob
from engines.metastore.models.credentials import MetastoreCredentials

class SparkRunner:

    @abstractmethod
    def run(self, config: SparkConfig, job: Job) -> Union[ExecutedJob, InvalidJob]:
        return InvalidJob("Runner not implemented")

class EmptySparkRunner(SparkRunner):

    def run(self, config: SparkConfig, job: Job) -> Union[ExecutedJob, InvalidJob]:
        return InvalidJob("Runner not implemented")

