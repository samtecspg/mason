
from util.logger import logger
from clients import EmptyClient
from typing import Optional, Union
from util.json_schema import validate_schema
from definitions import from_root
from os import path, environ
import re

def safe_interpolate_environment(config_doc: dict):
    return {k: interpolate_value(v) for k, v in config_doc.items()}

def interpolate_value(value: Union[str, dict]):

    SAFE_KEYS = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'AWS_REGION',
        'MASON_HOME',
        'KUBECONFIG',
        'GLUE_ROLE_ARN'
    ]

    r = re.compile(r'^\{\{[A-Z0-9_]+\}\}$')
    interpolated: Optional[str] = None
    if not value.__class__.__name__ == "dict": #TODO: deal with nested configuration structures
        # TODO: Fix type
        v:str = value # type: ignore
        if r.match(v):
            key = v.replace("{{", "").replace("}}", "")
            if key in SAFE_KEYS:
                interpolated = environ.get(key)
            else:
                interpolated = ""

    return interpolated or value

class Engine():

    def __init__(self, engine_type: str, config: Optional[dict]):
        self.client_name = (config or {}).get(f"{engine_type}_engine") or ""
        conf_doc = (config or {}).get("clients", {}).get(self.client_name, {}).get("configuration", {})
        self.config_doc = safe_interpolate_environment(conf_doc)
        self.valid = False

        schema_path = from_root(f"/clients/{self.client_name}/schema.json")

        # if len(self.config_doc) == 0 :
        #     if not self.client_name == "":
        #         logger.error()
        #         logger.error(f"No {self.client_name} client configuration for specified {engine_type}_engine")
        #         self.config_doc = {}
        #         self.client_name = "invalid"
        #     else:
        #         self.valid = True
        # else:
        if self.client_name == "":
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

    def to_dict(self):
        return {
            "client_name": self.client_name,
            "configuration": self.config_doc
        }

class EmptyEngine(Engine):
    def __init__(self):
        self.client_name = None

    def client(self):
        EmptyClient()

    def set_underlying_client(self, client):
        pass

    def to_dict(self):
        return {}
