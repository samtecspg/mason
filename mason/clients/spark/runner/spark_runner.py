
from abc import abstractmethod
from typing import Union

from mason.clients.spark.config import SparkConfig
from mason.engines.execution.models.jobs import ExecutedJob, Job, InvalidJob


class SparkRunner:

    @abstractmethod
    def run(self, config: SparkConfig, job: Job) -> Union[ExecutedJob, InvalidJob]:
        return InvalidJob(job, "Runner not implemented")

class EmptySparkRunner(SparkRunner):

    def run(self, config: SparkConfig, job: Job) -> Union[ExecutedJob, InvalidJob]:
        return InvalidJob(job, "Runner not implemented")

