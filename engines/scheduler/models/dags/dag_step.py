from typing import Optional, Union

from operators.valid_operator import ValidOperator
from configurations.valid_config import ValidConfig
from operators.operators import get_operator
from parameters import InputParameters
from util.environment import MasonEnvironment

# DagStep -> [DagStep, InvalidDagStep] -> [ValidDagStep, InvalidDagStep]

class DagStep:
    def __init__(self, step_config: dict, env: MasonEnvironment):
        self.namespace = step_config.get("namespace")
        self.command = step_config.get("command")
        self.operator = get_operator(env, self.namespace, self.command)

    def validate_definition(self) -> Union['DagStep', 'InvalidDagStep']:
        if self.operator:
            return self
        else:
            return InvalidDagStep(f"Invalid Dag Step:  Operator not found: {self.namespace}:{self.command}")

    def validate(self, env: MasonEnvironment, config: ValidConfig, parameters: InputParameters) -> Union['ValidDagStep', 'InvalidDagStep']:
        valid1 = self.validate_definition()
        if isinstance(valid1, DagStep):
            valid = self.operator.validate(config, parameters)
            if isinstance(valid, ValidOperator):
                return ValidDagStep(valid, env)
            else:
                return InvalidDagStep(f"Invalid Dag Step: Invalid Operator Definition: {valid.reason}")
        else:
            return valid1

class ValidDagStep:

    def __init__(self, operator: ValidOperator, env: MasonEnvironment):
        self.job = operator.job(env)

class InvalidDagStep:

    def __init__(self, reason: str):
        self.reason = reason