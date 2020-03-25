
from util.logger import logger
from clients import EmptyClient
from typing import Optional

class Engine():

    def __init__(self, engine_type: str, config: Optional[dict]):
        self.client_name = (config or {}).get(f"{engine_type}_engine") or ""
        self.config_doc = (config or {}).get("clients", {}).get(self.client_name, {}).get("configuration", {})

        if len(self.config_doc) == 0 :
            if not self.client_name == "":
                logger.error()
                logger.error(f"No {self.client_name} client configuration for specified {engine_type}_engine")

    def set_underlying_client(self, client):
        self.client.client.client = client

    def to_dict(self):
        return {
            "client_name": self.client_name,
            "configuration": self.config_doc
        }

class EmptyEngine(Engine):
    def __init__(self):
        self.client = EmptyClient()
        self.client_name = None

    def set_underlying_client(self, client):
        pass

    def to_dict(self):
        return {}
