from mason.clients.athena.execution import AthenaExecutionClient
from mason.clients.engines.invalid_client import InvalidClient
from mason.clients.engines.valid_client import ValidClient
from mason.clients.spark.execution import SparkExecutionClient
from mason.engines.engine import Engine


class ExecutionEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("execution", config)
        self.client = self.get_client()

    def get_client(self):
        client = self.validate()
        if isinstance(client, ValidClient):
            if client.client_name == "spark":
                return SparkExecutionClient(client.config)
            elif client.client_name == "athena":
                return AthenaExecutionClient(client.config)
            else:
                return InvalidClient(f"Client type not supported {client.client_name}")
        else:
            return client

