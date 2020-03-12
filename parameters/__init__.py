from typing import Optional, List
from util.yaml import parse_yaml
from util.printer import banner
from util.logger import logger
from operators.operator import Operator
from clients.response import Response
from util.dict import dedupe

import re

class Parameters:

    def parse_string(self, param_string: str) -> dict:
        pattern = r"([a-zA-Z0-9\-:_./]+:[a-zA-Z0-9\-:_./]+)"
        pattern_guide = "<param1>:<value1>,<param2>:<value2>"
        matches = re.findall(pattern, param_string)

        if len(matches) > 0:
            return dict(list([tuple(x.split(":")) for x in matches])) # type: ignore
        else:
            logger.error(f"Warning:  Parameter string does not conform to needed pattern: {pattern_guide}")
            return {}

    def __init__(self, parameters: Optional[str] = None, parameter_path: Optional[str] = None):
        self.parsed_parameters: dict = {}
        self.validated_parameters: dict = {}

        if parameters:
            parsed_parameters: dict = self.parse_string(parameters)
        elif parameter_path:
            parsed_parameters = parse_yaml(parameter_path) or {}
        else:
            logger.warning("Neither parameter string nor parameter path provided.")
            parsed_parameters = {}

        self.parsed_parameters = dedupe(parsed_parameters)

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

    def validate(self, op: Operator, response: Response = None):
        response = response or Response()
        required_params = set(op.required_parameters)
        provided_params = set(self.parsed_parameters.keys())
        diff = required_params.difference(provided_params)
        intersection = required_params.intersection(provided_params)
        self.add_valid(list(intersection))

        logger.info()
        validated = list(self.validated_parameters.keys())
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
