from typing import List

class Operator:

    def __init__(self, cmd: str, subcommand: str, required_parameters: List[str], supported_clients: List[str]):
        self.cmd = cmd
        self.subcommand = subcommand
        self.required_parameters = required_parameters
        self.supported_clients = supported_clients
