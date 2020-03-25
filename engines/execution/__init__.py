
from engines import Engine
from clients.spark.execution import SparkExecutionClient

class ExecutionEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("execution", config)
        self.client = self.get_client(self.client_name, self.config_doc)

    def get_client(self, client_name: str, config_doc: dict):
        if client_name == "spark":
            return SparkExecutionClient(config_doc)
        else:
            return None
