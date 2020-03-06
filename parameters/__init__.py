
from typing import Optional, List
from util.yaml import parse_yaml
from util.printer import banner

class Parameters:

    def __init__(self, parameters: Optional[str], parameter_path: Optional[str]):
        self.parsed_parameters: dict = {}
        self.validated_parameters: dict = {}
        if parameters:
            parameter_dict = dict(list([tuple(x.split(':')) for x in parameters.split(',')])) # type: ignore
            self.parsed_parameters = parameter_dict
        elif parameter_path:
            self.parsed_parameters = parse_yaml(parameter_path)
        else:
            self.parsed_parameters = {}

        if (self.parsed_parameters == {}) == False:
            print()
            banner("Parsed Parameters")
            print(self.parsed_parameters)
            print()

    def add_valid(self, validated_parameters: List[str]):
        validated = {k: v for k, v in self.parsed_parameters.items() if (k in validated_parameters)}
        self.validated_parameters = validated

    def safe_get(self, attribute: str) -> str:
        return self.validated_parameters.get(attribute, "")

    def unsafe_get(self, attribute: str) -> Optional[str]:
        return self.parsed_parameters.get(attribute, None)
