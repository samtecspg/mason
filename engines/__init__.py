
from util.logger import logger
from clients import EmptyClient
from typing import Optional
from util.json_schema import validate_schema
from definitions import from_root
from os import path

class Engine():

    def __init__(self, engine_type: str, config: Optional[dict]):
        self.client_name = (config or {}).get(f"{engine_type}_engine") or ""
        self.config_doc = (config or {}).get("clients", {}).get(self.client_name, {}).get("configuration", {})
        self.valid = False

        schema_path = from_root(f"/clients/{self.client_name}/schema.json")

        if len(self.config_doc) == 0 :
            if not self.client_name == "":
                logger.error()
                logger.error(f"No {self.client_name} client configuration for specified {engine_type}_engine")
                self.config_doc = {}
                self.client_name = "invalid"
            else:
                self.valid = True
        else:
            if path.exists(schema_path):
                valid = validate_schema(self.config_doc, schema_path)
            else:
                logger.warning(f"Specified schema at {schema_path} does not exist")
                valid = True

            if not valid:
                if not self.client_name == "":
                    logger.error(f"Invalid configuration for client {self.client_name}")
                    self.client_name = "invalid"
                    self.config_doc = {}
                else:
                    self.valid = True
            else:
                self.valid = True

    def set_underlying_client(self, client):
        self.client.client.client = client

    def underlying_client(self):
        return self.client.client.client

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
