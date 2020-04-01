
from abc import abstractmethod
from clients.spark.config import SparkConfig

class SparkRunner:

    @abstractmethod
    def run(self, config: SparkConfig):
        raise NotImplementedError("Runner not implemented")

class EmptySparkRunner(SparkRunner):

    def run(self, config: SparkConfig):
        raise NotImplementedError("Runner not implemented")


