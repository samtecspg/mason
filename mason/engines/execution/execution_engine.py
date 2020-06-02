from mason.engines.engine import Engine

class ExecutionEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("execution", config)
        self.client = self.get_client()

    def get_client(self):

        client = self.validate()
        from mason.clients.engines.valid_client import ValidClient
        if isinstance(client, ValidClient):
            if client.client_name == "spark":
                from mason.clients.spark.execution import SparkExecutionClient
                return SparkExecutionClient(client.config)
            elif client.client_name == "athena":
                from mason.clients.athena.execution import AthenaExecutionClient
                return AthenaExecutionClient(client.config)
            else:
                from mason.clients.engines.invalid_client import InvalidClient
                return InvalidClient(f"Client type not supported {client.client_name}")
        else:
            return client

