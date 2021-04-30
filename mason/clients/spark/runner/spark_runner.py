
from abc import abstractmethod
from typing import Union, Tuple

from mason.clients.response import Response
from mason.clients.spark.config import SparkConfig
from mason.engines.execution.models.jobs import ExecutedJob, Job, InvalidJob

class SparkRunner:

    @abstractmethod
    def run(self, config: SparkConfig, job: Job) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        raise NotImplementedError("Runner not implemented")

class EmptySparkRunner(SparkRunner):

    def run(self, config: SparkConfig, job: Job) -> Tuple[Union[ExecutedJob, InvalidJob], Response]:
        return InvalidJob("No Spark Runner Specified"), Response()

