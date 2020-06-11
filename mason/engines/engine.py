from abc import abstractmethod
from typing import Union

from mason.clients.engines.invalid_client import InvalidClient
from mason.clients.engines.valid_client import ValidClient, EmptyClient
from mason.configurations import REDACTED_KEYS
from mason.definitions import from_root
from mason.engines import safe_interpolate_environment
from mason.util.dict import sanitize
from mason.util.json_schema import validate_schema, ValidSchemaDict

class Engine:

    def __init__(self, engine_type: str, config: dict):
        self.engine_type = engine_type
        self.config_doc = config
        self.client_name: str = self.config_doc.get(f"{self.engine_type}_engine", "")
        self.config = safe_interpolate_environment(config.get("clients", {}).get(self.client_name, {}).get("configuration", {}))

    def validate(self) -> Union[ValidClient, EmptyClient, InvalidClient]:
        if not self.client_name == "":
            schema_path = from_root(f"/clients/{self.client_name}/schema.json")

            schema = validate_schema(self.config, schema_path)

            if isinstance(schema, ValidSchemaDict):
                return ValidClient(self.client_name, self.config)
            else:
                return InvalidClient(f"Invalid Schema Definition: {schema_path}.  Reason:  {schema.reason}")
        else:
            return EmptyClient()

    def to_dict(self):
        return {
            'client_name': self.client_name,
            'configuration': sanitize(self.config, REDACTED_KEYS)
        }

