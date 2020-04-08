from configurations import Config
from clients.response import Response
from typing import Dict, Optional, List
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

    def find_configuration(self, configs: List[Config], response: Response):
        configuration: Optional[Config] = None
        for config in configs:
            vc = self.validate_configuration(config, response, False)
            if vc[0] == True:
                configuration = config
                break
        if configuration == None:
            response.add_error(f"No matching configuration for operator {self.cmd} {self.subcommand}.  Check operator.yml for supported configurations.")
        return configuration, response

    def validate_configuration(self, config: Config, response: Response, log_error: bool = True):
        test = False
        for ses in self.supported_configurations:
            test = ses.validate_coverage(config)
            if test:
                break
        if not test and log_error:
            response.add_error("Configuration not supported by configured engines.  Check operator.yaml for supported engine configurations.")
        return test, response

    def to_dict(self):
        return {
            'cmd': self.cmd,
            'subcommand': self.subcommand,
            'description': self.description,
            'parameters': self.parameters,
            'supported_configurations': list(map(lambda x: x.all, self.supported_configurations))
        }
