from typing import Optional, List
from util.yaml import parse_yaml
from util.printer import banner
from util.logger import logger
from util.dict import dedupe

import re

class Parameters:

    def parse_string(self, param_string: str) -> dict:
        pattern = r"([^,^:]+:[^,^:]+)"
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

