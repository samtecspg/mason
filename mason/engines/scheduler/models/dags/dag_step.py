from typing import Union, List, Optional


from mason.configurations.configurations import get_config_by_id
from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
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
        self.retry_method: Optional[str] = step_config.get("retry") # type: ignore
        self.retry_max: Optional[int] = step_config.get("retry_max") # type: ignore

    def validate(self, env: MasonEnvironment, parameters: WorkflowParameters, all_step_ids: List[str]) -> Union[ValidDagStep, InvalidDagStep]:
        diff = set(self.dependencies).difference(all_step_ids)
        if len(diff) == 0:
            wfp = parameters.get(self.id)
            if wfp:
                config = get_config_by_id(env, wfp.config_id)
                if config:
                    operator_params: InputParameters = wfp.parameters
                    operator = get_operator(env, self.namespace or "", self.command or "")
                    if operator:
                        valid = operator.validate(config, operator_params)
                        if isinstance(valid, ValidOperator):
                            return ValidDagStep(self.id, valid, self.dependencies, self.retry_method, self.retry_max)
                        else:
                            return InvalidDagStep(f"Invalid Dag Step {self.id}: Invalid Operator Definition: {valid.reason}")
                    else:
                        return InvalidDagStep(f"Invalid Dag Step {self.id}: Operator not found {self.namespace}:{self.command}")
                else:
                    return InvalidDagStep(f"Invalid Dag Step {self.id}: Config not found with id {wfp.config_id}")
            else:
                messages = ", ".join(list(map(lambda p: p.reason, parameters.invalid)))
                return InvalidDagStep(f"Workflow Parameters for step:{self.id} not specified. Invalid Parameters: {messages}")
        else:
            return InvalidDagStep(f"Undefined dependent steps: {diff}")
