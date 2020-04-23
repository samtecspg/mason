from configurations import Config
from clients.response import Response
from typing import Dict, Optional, List, Tuple
from operators.supported_engines import from_array, SupportedEngineSet
from parameters import Parameters
from util.logger import logger
from util.printer import banner
from importlib import import_module
from util.environment import MasonEnvironment


class Operator:

    def __init__(self, cmd: str, subcommand: str, description: str, parameters: dict, supported_engine_sets: List[Dict[str, str]]):
        self.cmd = cmd
        self.subcommand = subcommand
        self.description = description
        self.parameters: dict = parameters
        self.supported_configurations: List[SupportedEngineSet] = from_array(supported_engine_sets)

    def required_parameters(self):
        return self.parameters.get("required", [])

    def find_configurations(self, configs: List[Config], response: Response) -> Tuple[List[Config], Response]:
        configurations: List[Config] = []
        for config in configs:
            response = self.validate_configuration(config, Response())
            if not response.errored():
                configurations.append(config)
        if len(configurations) == 0:
            response.add_error(f"No matching configuration for operator {self.cmd} {self.subcommand}.  Check operator.yml for supported configurations.")
        return configurations, response

    def run(self, env: MasonEnvironment, config: Config, parameters: Parameters, response: Response) -> Response:

        self.validate(config, parameters, response)

        if not response.errored():
            try:
                mod = import_module(f'{env.operator_module}.{self.cmd}.{self.subcommand}')
                response = mod.run(env, config, parameters, response)  # type: ignore
            except ModuleNotFoundError as e:
                response.add_error(f"Module Not Found: {e}")

        return response

    def validate(self, config: Config, parameters: Parameters, response: Response) -> Response:
        response = self.validate_params(parameters, response)
        response = self.validate_configuration(config, response)
        return response

    def validate_params(self, params: Parameters, response: Response):
        required_params = set(self.required_parameters())
        provided_params = set(params.parsed_parameters.keys())
        diff = required_params.difference(provided_params)
        intersection = required_params.intersection(provided_params)
        params.add_valid(list(intersection))

        logger.info()
        validated = list(params.validated_parameters.keys())
        missing = list(diff)

        banner(f"Parameters Validation:")
        if len(validated) > 0:
            logger.info(f"Validated: {validated}")
        if len(missing) > 0:
            logger.info(f"Missing: {missing}")
        logger.info()

        if len(diff) > 0:
            dp = ", ".join(list(diff))
            response.add_error(f"Missing required parameters: {dp}")
            response.set_status(400)
        return response

    def validate_configuration(self, config: Config, response: Response):
        test = False
        for ses in self.supported_configurations:
            test = ses.validate_coverage(config)
            if test:
                break
        if not test:
            response.add_error("Configuration not supported by configured engines.  Check operator.yaml for supported engine configurations.")
        return response

    def to_dict(self):
        return {
            'cmd': self.cmd,
            'subcommand': self.subcommand,
            'description': self.description,
            'parameters': self.parameters,
            'supported_configurations': list(map(lambda x: x.all, self.supported_configurations))
        }
