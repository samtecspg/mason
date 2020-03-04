from configurations.base_config import BaseConfig
from clients import Client

class ExecutionConfig(BaseConfig):

    def __init__(self, config: dict):
        self.client_name = config.get("execution_client", "None")
        self.client = Client().get(self.client_name, config.get("clients", {}))
