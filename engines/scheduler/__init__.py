
from clients.glue.scheduler import GlueSchedulerClient
from clients.engines.scheduler import EmptySchedulerClient
from engines import Engine

class SchedulerEngine(Engine):

    def __init__(self, config: dict, valid: bool = True):
        super().__init__("scheduler", config)
        if valid and self.valid:
            self.client = self.get_client(self.client_name, self.config_doc)
        else:
            self.client = EmptySchedulerClient()

    def get_client(self, client_name: str, config_doc: dict):
        if client_name == "glue":
            return GlueSchedulerClient(config_doc)
        else:
            return EmptySchedulerClient()
