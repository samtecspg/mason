from typing import List, Union

from engines.scheduler.models.dags.dag_step import DagStep, InvalidDagStep, ValidDagStep
from parameters import WorkflowParameters
from util.environment import MasonEnvironment
from util.json_schema import sequence

from util.list import flatten

class Dag:

    #  Dag -> [Dag, InvalidDag] -> [ValidDag, InvalidDag]

    def __init__(self, dag_config: List[dict]):
        self.steps = flatten(list(map(lambda step: DagStep(step), dag_config)))

    def validate(self, env: MasonEnvironment, all_parameters: WorkflowParameters) -> Union['ValidDag', 'InvalidDag']:
        validated: List[Union[ValidDagStep, InvalidDagStep]] = flatten(list(map(lambda s: s.validate(env, all_parameters), self.steps)))
        valid_steps, invalid_steps = sequence(validated, ValidDagStep, InvalidDagStep)

        return self.validate_dag(valid_steps, invalid_steps)

    def validate_dag(self, valid_steps: List[ValidDagStep], invalid_steps: List[InvalidDagStep]) -> Union['ValidDag', 'InvalidDag']:
        if len(valid_steps) == 0:
            return InvalidDag("No valid DAG steps", valid_steps, invalid_steps)
        else:
            #  TODO: Validate that is indeed a valid dag
            return ValidDag(valid_steps, invalid_steps)

class ValidDag:

    def __init__(self, valid_steps: List[ValidDagStep], invalid_steps: List[InvalidDagStep]):
        self.valid_steps = valid_steps
        self.invalid_steps = invalid_steps

    def to_dict(self) -> dict:
        # TODO: Find Sensible way to display dag
        return {}

class InvalidDag:

    def __init__(self, reason: str, valid_steps: List[ValidDagStep], invalid_steps: List[InvalidDagStep]):
        self.valid_steps = valid_steps
        self.invalid_steps = invalid_steps
        invalid_step_reasons: List[str] = list(map(lambda i: i.reason, self.invalid_steps))
        self.reason = reason + " Invalid Dag Steps: " + " ,".join(invalid_step_reasons)

