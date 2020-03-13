
from clients.engines.execution import EmptyExecutionClient
from engines import Engine
from util.logger import logger

class ExecutionEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("execution", config)
        self.client = self.get_client(self.client_name, self.config_doc)

    def get_client(self, client_name: str, config_doc: dict):
        logger.info()
        logger.info(f"Empty Execution Client: {client_name} {config_doc}")
        logger.info()
        return EmptyExecutionClient()

