from typing import List
from configurations import Config
from clients.response import Response
from typing import Dict
from operators.supported_engines import from_array, SupportedEngineSet

class Operator:

    def __init__(self, cmd: str, subcommand: str, description: str, parameters: dict, supported_engine_sets: List[Dict[str, str]]):
        self.cmd = cmd
        self.subcommand = subcommand
        self.description = description
        self.parameters: dict = parameters
        self.supported_configurations: List[SupportedEngineSet] = from_array(supported_engine_sets)

    def required_parameters(self):
        return self.parameters.get("required", [])

    def validate_configuration(self, config: Config, response: Response):
        test = False
        for ses in self.supported_configurations:
            test = ses.validate_coverage(config)
            if test:
                break
        if not test:
            response.add_error("Configuration not supported by any supported engine configurations.  Check operator.yaml for supported engine configurations.")
        return test, response
