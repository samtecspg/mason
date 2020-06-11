from typing import Union, List, Optional

from mason.configurations.configurations import get_config_by_id
from mason.operators.operators import get_operator
from mason.operators.valid_operator import ValidOperator
from mason.parameters.input_parameters import InputParameters
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.util.environment import MasonEnvironment

class DagStep:
    def __init__(self, step_config: dict):
        self.id: str = str(step_config.get("id")) # type: ignore
        self.namespace: str = step_config.get("namespace") # type: ignore
        self.command: str = step_config.get("command") # type: ignore
        self.dependencies: List[str] = step_config.get("dependencies", []) # type: ignore
        self.retry: Optional[str] = step_config.get("retry") # type: ignore
        
    def validate(self, env: MasonEnvironment, parameters: WorkflowParameters) -> Union['ValidDagStep', 'InvalidDagStep']:
        wfp = parameters.get(self.id)
        if wfp:
            config = get_config_by_id(env, wfp.config_id)
            if config:
                operator_params: InputParameters = wfp.parameters
                operator = get_operator(env.operator_home, self.namespace or "", self.command or "")
                if operator:
                    valid = operator.validate(config, operator_params)
                    if isinstance(valid, ValidOperator):
                        return ValidDagStep(self.id, valid, self.dependencies, self.retry)
                    else:
                        return InvalidDagStep(f"Invalid Dag Step: Invalid Operator Definition: {valid.reason}")
                else:
                    return InvalidDagStep(f"Invalid Dag Step: Operator not found {self.namespace}:{self.command}")
            else:
                return InvalidDagStep(f"Invalid Dag Step: Config not found with id {wfp.config_id}")
        else:
            messages = ", ".join(list(map(lambda p: p.reason, parameters.invalid)))
            return InvalidDagStep(f"Workflow Parameters for step:{self.id} not specified. Invalid Parameters: {messages}")

class ValidDagStep:

    def __init__(self, id: str, operator: ValidOperator, dependencies: List[str], retry: Optional[str]):
        self.id = id
        self.operator = operator
        self.dependencies = dependencies
        self.retry = retry

class InvalidDagStep:

    def __init__(self, reason: str):
        self.reason = reason