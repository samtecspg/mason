
from typing import Optional, List
from util.yaml import parse_yaml
from util.printer import banner
from util.logger import logger
from configurations.valid_operator import ValidOperator
from clients.response import Response

class Parameters:

    def __init__(self, parameters: Optional[str] = None, parameter_path: Optional[str] = None):
        self.parsed_parameters: dict = {}
        self.validated_parameters: dict = {}

        if parameters:
            parameter_dict: dict = dict(list([tuple(x.split(':')) for x in parameters.split(',')])) # type: ignore
            self.parsed_parameters = parameter_dict
        elif parameter_path:
            self.parsed_parameters = parse_yaml(parameter_path)
        else:
            self.parsed_parameters = {}

        if not (self.parsed_parameters == {}):
            logger.info()
            banner("Parsed Parameters")
            logger.info(str(self.parsed_parameters))
            logger.info()

    def add_valid(self, validated_parameters: List[str]):
        validated = {k: v for k, v in self.parsed_parameters.items() if (k in validated_parameters)}
        self.validated_parameters = validated

    def safe_get(self, attribute: str) -> str:
        return self.validated_parameters.get(attribute, "")

    def unsafe_get(self, attribute: str) -> Optional[str]:
        return self.parsed_parameters.get(attribute, None)

    def validate(self, op: ValidOperator, response: Response):
        required_params = set(op.required_parameters)
        provided_params = set(self.parsed_parameters.keys())
        sym_diff = required_params.symmetric_difference(provided_params)
        intersection = required_params.intersection(provided_params)
        self.add_valid(list(intersection))

        logger.info()
        validated = list(self.validated_parameters.keys())
        missing = list(sym_diff)
        banner(f"Parameters Validation:")
        if len(validated) > 0:
            logger.info(f"Validated: {validated}")
        if len(missing) > 0:
            logger.info(f"Missing: {missing}")
        logger.info()

        if len(sym_diff) > 0:
            dp = ", ".join(list(sym_diff))
            response.add_error(f"Missing required parameters: {dp}")
        return response
