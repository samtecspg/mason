import shutil
from os import path, remove
import importlib.util
from sys import path as sys_path
from typing import Dict, Optional, List, Union

from mason.configurations.valid_config import ValidConfig
from mason.configurations.invalid_config import InvalidConfig
from mason.operators.invalid_operator import InvalidOperator
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.supported_engines import from_array, SupportedEngineSet
from mason.operators.valid_operator import ValidOperator
from mason.parameters.parameters import Parameters
from mason.parameters.input_parameters import InputParameters
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.logger import logger
from mason.util.string import to_class_case


class Operator:

    def __init__(self, namespace: str, command: str, description: str, parameters: dict, supported_configurations: List[Dict[str, str]], source_path: Optional[str] = None):
        self.namespace = namespace
        self.command = command
        self.description = description
        self.parameters: Parameters = Parameters(parameters)
        self.supported_configurations: List[SupportedEngineSet] = from_array(supported_configurations)
        if source_path:
            self.source_path = source_path

    def module(self) -> Union[OperatorDefinition, InvalidOperator]:
        sys_path.append(self.source_path)
        spec = importlib.util.spec_from_file_location("module.name", self.source_path.replace("operator.yaml", "__init__.py"))
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod) # type: ignore
        try:
            classname = to_class_case(f"{self.namespace}_{self.command}")
            operator_class = getattr(mod, classname)()
            if isinstance(operator_class, OperatorDefinition):
                return operator_class
            else:
                return InvalidOperator("Invalid Operator definition.  See operators/operator_definition.py")
        except AttributeError as e:
            return InvalidOperator(f"Operator has no attribute {classname}")

    def validate(self, config: ValidConfig, parameters: InputParameters) -> Union[ValidOperator, InvalidOperator]:
        validated_params: ValidatedParameters = self.parameters.validate(parameters)
        validated_config = self.validate_config(config)

        a: Union[ValidOperator, InvalidOperator]
        if validated_params.has_invalid():
            return InvalidOperator(f"Invalid parameters.  {validated_params.messages()}")
        elif isinstance(validated_config, InvalidConfig):
            return InvalidOperator(f"Invalid config: {validated_config.reason}")
        else:
            return ValidOperator(self.namespace, self.command,self.supported_configurations,self.description, validated_params, validated_config)

    def validate_config(self, config: ValidConfig) -> Union[ValidConfig, InvalidConfig]:
        test = False
        for ses in self.supported_configurations:
            test = ses.validate_coverage(config)
            if test:
                break

        if test:
            return config
        else:
            return InvalidConfig(config.config, f"Configuration {config.id} not supported by configured engines for operator {self.namespace}:{self.command}.  Check operator.yaml for supported engine configurations.")

    def register_to(self, operator_home: str, force: bool = False):
        if self.source_path:
            dir = path.dirname(self.source_path)
            tree_path = ("/").join([operator_home.rstrip("/"), self.namespace, self.command + "/"])
            if not path.exists(tree_path):
                logger.info(f"Valid operator definition.  Registering {dir} to {tree_path}")
                shutil.copytree(dir, tree_path)
            else:
                if force:
                    shutil.rmtree(tree_path)
                    logger.info(f"Valid operator definition.  Registering {dir} to {tree_path}")
                    shutil.copytree(dir, tree_path)
                else:
                    logger.error(f"Operator definition already exists {self.namespace}:{self.command}")

        else:
            logger.error("Source path not found for operator, run validate_operators to populate")

    def to_dict(self):
        return {
            'namespace': self.namespace,
            'command': self.command,
            'description': self.description,
            'parameters': self.parameters.to_dict(),
            'supported_configurations': list(map(lambda s: s.all, self.supported_configurations))
        }


def emptyOperator():
    Operator("", "", "", {}, [])