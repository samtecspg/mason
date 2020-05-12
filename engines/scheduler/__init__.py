from clients.engines.invalid_client import InvalidClient
from clients.engines.valid_client import ValidClient
from clients.glue.scheduler import GlueSchedulerClient
from engines.engine import Engine

class SchedulerEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("scheduler", config)
        self.client = self.get_client()

    def get_client(self):
        client = self.validate()
        if isinstance(client, ValidClient):
            if client.client_name == "glue":
                return GlueSchedulerClient(client.config)
            else:
                return InvalidClient(f"Client type not supported: {client.client_name}")
        else:
            return client

