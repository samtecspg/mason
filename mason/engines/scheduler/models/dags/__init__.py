from typing import List, Union

from mason.engines.scheduler.models.dags.dag_step import DagStep, InvalidDagStep, ValidDagStep
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.util.environment import MasonEnvironment

from mason.util.list import flatten, sequence


class Dag:

    def __init__(self, dag_config: List[dict]):
        self.steps = flatten(list(map(lambda step: DagStep(step), dag_config)))

    def validate(self, env: MasonEnvironment, all_parameters: WorkflowParameters) -> Union['ValidDag', 'InvalidDag']:
        validated: List[Union[ValidDagStep, InvalidDagStep]] = flatten(list(map(lambda s: s.validate(env, all_parameters), self.steps)))
        valid_steps, invalid_steps = sequence(validated, ValidDagStep, InvalidDagStep)

        return self.validate_dag(valid_steps, invalid_steps)

    def validate_dag(self, valid_steps: List[ValidDagStep], invalid_steps: List[InvalidDagStep]) -> Union['ValidDag', 'InvalidDag']:
        if len(valid_steps) == 0:
            return InvalidDag("No valid DAG steps. ", valid_steps, invalid_steps)
        else:
            #  TODO: Validate that is indeed a valid dag
            return ValidDag(valid_steps, invalid_steps)

class ValidDag:

    def __init__(self, valid_steps: List[ValidDagStep], invalid_steps: List[InvalidDagStep]):
        self.valid_steps = valid_steps
        self.invalid_steps = invalid_steps

    def to_dict(self) -> dict:
        return {}

    def display(self) -> str:
        #  TODO: Print out full dag diagram with multi-steps
        vs = self.valid_steps
        vs0 = vs[0]
        return f"{vs0.id}>> {vs0.operator.display_name()}"

class InvalidDag:

    def __init__(self, reason: str, valid_steps: List[ValidDagStep], invalid_steps: List[InvalidDagStep]):
        self.valid_steps = valid_steps
        self.invalid_steps = invalid_steps
        invalid_step_reasons: List[str] = list(map(lambda i: i.reason, self.invalid_steps))
        self.reason = reason + " Invalid Dag Steps: " + " ,".join(invalid_step_reasons)

