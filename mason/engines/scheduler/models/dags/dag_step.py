from typing import Union, List, Optional

from mason.configurations.config import Config
from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
from mason.operators.operator import Operator
from mason.operators.valid_operator import ValidOperator
from mason.parameters.operator_parameters import OperatorParameters
from mason.parameters.workflow_parameters import WorkflowParameters
from mason import resources
from mason.util.environment import MasonEnvironment


class DagStep:
    def __init__(self, step_config: dict):
        self.id: str = str(step_config.get("id")) # type: ignore
        self.namespace: str = step_config.get("namespace") # type: ignore
        self.command: str = step_config.get("command") # type: ignore
        self.dependencies: List[str] = step_config.get("dependencies", []) # type: ignore
        self.retry_method: Optional[str] = step_config.get("retry") # type: ignore
        self.retry_max: Optional[int] = step_config.get("retry_max") # type: ignore

    def validate(self, env: MasonEnvironment, parameters: WorkflowParameters, all_step_ids: List[str]) -> Union[ValidDagStep, InvalidDagStep]:
        diff = set(self.dependencies).difference(all_step_ids)
        if len(diff) == 0:
            wfp = parameters.get(self.id)
            if wfp:
                res = resources.base.Resources(env) # type: ignore   #TODO: fix this
                config = res.get_config(wfp.config_id)
                if isinstance(config, Config):
                    operator_params: OperatorParameters = wfp.parameters
                    operator = res.get_operator(self.namespace, self.command)
                    if isinstance(operator, Operator):
                        valid = operator.validate(config, operator_params)
                        if isinstance(valid, ValidOperator):
                            return ValidDagStep(self.id, valid, self.dependencies, self.retry_method, self.retry_max)
                        else:
                            return InvalidDagStep(f"Invalid Dag Step {self.id}: Invalid Operator Definition: {valid.reason}")
                    else:
                        return InvalidDagStep(f"Invalid Dag Step {self.id}: {operator.get_message()}")
                else:
                    return InvalidDagStep(f"Invalid Dag Step {self.id}: {config.get_message()}")
            else:
                messages = ", ".join(list(map(lambda p: p.reason, parameters.invalid)))
                return InvalidDagStep(f"Workflow Parameters for step:{self.id} not specified. Invalid Parameters: {messages}")
        else:
            return InvalidDagStep(f"Undefined dependent steps: {diff}")
        
    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'namespace': self.namespace,
            'command': self.command,
            'dependencies': self.dependencies,
            'retry_method': self.retry_method,
            'retry_max': self.retry_max,
        }