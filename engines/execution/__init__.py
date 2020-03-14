
from clients.engines.execution import EmptyExecutionClient
from engines import Engine

class ExecutionEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("execution", config)
        self.client = self.get_client(self.client_name, self.config_doc)

    def get_client(self, client_name: str, config_doc: dict):
        return EmptyExecutionClient()

