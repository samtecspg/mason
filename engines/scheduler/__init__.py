
from clients.glue.scheduler import GlueSchedulerClient
from engines import Engine
from clients.engines.scheduler import SchedulerClient, EmptySchedulerClient

class SchedulerEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("scheduler", config)
        self.client = self.get_client(self.client_name, self.config_doc)

    def get_client(self, client_name: str, config_doc: dict):
        if client_name == "glue":
            return GlueSchedulerClient(config_doc)
        else:
            return EmptySchedulerClient()
