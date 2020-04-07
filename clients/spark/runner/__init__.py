
from abc import abstractmethod
from clients.spark.config import SparkConfig
from clients.response import Response

class SparkRunner:

    @abstractmethod
    def run(self, config: SparkConfig, params: dict, response: Response):
        raise NotImplementedError("Runner not implemented")

class EmptySparkRunner(SparkRunner):

    def run(self, config: SparkConfig, params: dict, response: Response):
            raise NotImplementedError("Runner not implemented")


