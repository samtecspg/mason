from typing import Union, Optional

from engines.execution.models.jobs import Job

from configurations.configurations import get_config_by_id
from operators.valid_operator import ValidOperator
from operators.operators import get_operator
from parameters import WorkflowParameters, InputParameters
from util.environment import MasonEnvironment

from util.list import get


class DagStep:
    def __init__(self, step_config: dict):
        self.id = str(step_config.get("id"))
        self.namespace = step_config.get("namespace")
        self.command = step_config.get("command")

    def validate(self, env: MasonEnvironment, parameters: WorkflowParameters) -> Union['ValidDagStep', 'InvalidDagStep']:
        if len(parameters.parameters) == 1:
            wfp = get(parameters.parameters, 0) # for parse string simple one step case
        else:
            wfp = get([p for p in parameters.parameters if p.step == self.id], 0)

        if wfp:
            config = get_config_by_id(env, wfp.config_id)
            if config:
                operator_params: InputParameters = wfp.parameters
                operator = get_operator(env, self.namespace or "", self.command or "")
                if operator:
                    valid = operator.validate(config, operator_params)
                    if isinstance(valid, ValidOperator):
                        return ValidDagStep(valid)
                    else:
                        return InvalidDagStep(f"Invalid Dag Step: Invalid Operator Definition: {valid.reason}")
                else:
                    return InvalidDagStep(f"Invalid Dag Step: Operator not found {self.namespace}:{self.command}")
            else:
                return InvalidDagStep(f"Invalid Dag Step: Config not found with id {wfp.config_id}")
        else:
            return InvalidDagStep(f"Workflow Parameters for step {self.id} not specified")

class ValidDagStep:

    def __init__(self, operator: ValidOperator):
        self.operator = operator

class InvalidDagStep:

    def __init__(self, reason: str):
        self.reason = reason