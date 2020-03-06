
from configurations import Config
from parameters import Parameters
from clients.response import Response

class Operation:

    def run(self, config: Config, parameters: Parameters, response: Response):
        print(f"Undefined operator definition {config} {parameters} {response}")

