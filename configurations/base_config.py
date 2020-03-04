
from clients import Client

class BaseConfig:

    def __init__(self, config: dict, type: str):
        self.client_name = config.get(type, "None")
        self.client = Client().get(self.client_name, config.get("clients", {}))

    def to_dict(self):
        if self.client:
            filt = {key: value for (key, value) in self.client.__dict__.items() if not key == "client"}
            return {
                'client': self.client_name,
                'configuration': filt
            }
        else:
            return {}

