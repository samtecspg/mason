
from typing import List

class ValidOperator:

    def __init__(self, cmd: str, subcommand: str, required_parameters: List[str]):
        self.cmd = cmd
        self.subcommand = subcommand
        self.required_parameters = required_parameters
