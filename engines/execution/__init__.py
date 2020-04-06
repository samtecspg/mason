
from engines import Engine
from clients.spark.execution import SparkExecutionClient
from clients.engines.execution import EmptyExecutionClient

class ExecutionEngine(Engine):

    def __init__(self, config: dict, valid: bool = True):
        super().__init__("execution", config)
        if valid and self.valid:
            self.client = self.get_client(self.client_name, self.config_doc)
        else:
            self.client = EmptyExecutionClient()

    def get_client(self, client_name: str, config_doc: dict):
        if client_name == "spark":
            return SparkExecutionClient(config_doc)
        else:
            return EmptyExecutionClient()
