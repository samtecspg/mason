from typing import List
from configurations import Config
from util.logger import logger
from clients.response import Response
from typing import Dict
from util.list import flatten_array

class Operator:

    def __init__(self, cmd: str, subcommand: str, description: str, parameters: dict, supported_clients: Dict[str, List[str]]):
        self.cmd = cmd
        self.subcommand = subcommand
        self.description = description
        self.parameters: dict = parameters
        self.supported_clients = supported_clients

    def required_parameters(self):
        return self.parameters.get("required", [])

    # TODO: Tighten this up, clients should be engine specific
    def validate_configuration(self, config: Config, response: Response):
        test = len(set(self.all_clients()).difference(set(config.client_names()))) == 0

        if test:
            logger.info(f"Operator {self.cmd}:{self.subcommand} supported by configured clients")
            return self, response
        else:
            response.add_error(f"Configured clients {config.client_names()} not supporting operator: {self.supported_clients}")
            return None, response

    def all_clients(self) -> List[str]:
        return list(set(flatten_array(list(self.supported_clients.values()))))



