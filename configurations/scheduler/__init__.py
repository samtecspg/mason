
from clients import Client

class SchedulerConfig(object):

    def __init__(self, config: dict):
        self.client_name = config.get("scheduler_client", "")
        self.client = Client().get(self.client_name, config.get("clients", {}))

    def to_dict(self):
        return {
            'client': self.client_name,
            'configuration': self.client.__dict__
        }


