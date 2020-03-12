from typing import List

class Operator:

    def __init__(self, cmd: str, subcommand: str, description: str, parameters: dict, supported_clients: List[str]):
        self.cmd = cmd
        self.subcommand = subcommand
        self.description = description
        self.parameters: dict = parameters
        self.supported_clients = supported_clients

    def required_parameters(self):
        return self.parameters.get("required", [])
