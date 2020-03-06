
from util.logger import logger

class Engine:

    def __init__(self, engine_type: str, config: dict):
        self.client_name =  config.get(f"{engine_type}_engine") or ""

        self.config_doc = config.get("clients", {}).get(self.client_name, {}).get("configuration", {})

        if len(self.config_doc) == 0 :
            if not self.client_name == "":
                logger.error(f"No {self.client_name} client configuration for specified {engine_type}_engine")

    def to_dict(self):
        return {
            "client_name": self.client_name,
            "configuration": self.config_doc
        }
