from typing import Union

from definitions import from_root
from engines import safe_interpolate_environment
from clients.engines.invalid_client import InvalidClient
from clients.engines.valid_client import ValidClient, EmptyClient
from util.json_schema import validate_schema, ValidSchemaDict

class Engine:

    def __init__(self, engine_type: str, config: dict):
        self.engine_type = engine_type
        self.config_doc = config
        self.client_name: str = self.config_doc.get(f"{self.engine_type}_engine", "")
        self.config = safe_interpolate_environment(config.get("clients", {}).get(self.client_name, {}).get("configuration", {}))

    def validate(self) -> Union[ValidClient, InvalidClient, EmptyClient]:
        if not self.client_name == "":

            # TODO:  Improve this
            if (self.config_doc or {}).get("testing", False):
                schema_path = from_root(f"/test/support/clients/{self.client_name}/schema.json")
            else:
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
            'configuration': self.config
        }

