from typing import List

class Operator:

    def __init__(self, cmd: str, subcommand: str, description: str, parameters: dict, supported_clients: List[str]):
        self.cmd = cmd
        self.subcommand = subcommand
        self.description = description
        self.parameters = parameters
        self.supported_clients = supported_clients

    def required_parameters(self): List[str] = self.parameters.get("required", [])
