from typing import List, Union

from configurations.valid_config import ValidConfig
from engines.scheduler.models.dags.dag_step import DagStep, InvalidDagStep, ValidDagStep
from parameters import InputParameters
from util.environment import MasonEnvironment

from util.list import flatten, split_type

class Dag:

    #  Dag -> [Dag, InvalidDag] -> [ValidDag, InvalidDag]

    def __init__(self, dag_config: List[dict]):
        self.steps = flatten(list(map(lambda step: DagStep(step), dag_config)))

    def validate_definition(self) -> Union['Dag', 'InvalidDag']:
        validated: List[Union[DagStep, InvalidDagStep]] = flatten(list(map(lambda s: s.validate_definition(), self.steps)))
        valid_steps, invalid_steps = split_type(validated)
        if len(invalid_steps) > 0:
            return InvalidDag(valid_steps, invalid_steps)
        else:
            return self

    def validate(self, env: MasonEnvironment, config: ValidConfig, parameters: InputParameters) -> Union['ValidDag', 'InvalidDag']:
        validated: List[Union[ValidDagStep, InvalidDagStep]] = flatten(list(map(lambda s: s.validate(env, config, parameters), self.steps)))
        valid_steps, invalid_steps = split_type(validated)
        return self.validate_dag(valid_steps, invalid_steps)

    def validate_dag(self, valid_steps: List[ValidDagStep], invalid_steps: List[InvalidDagStep]) -> Union['ValidDag', 'InvalidDag']:
        #  TODO: Validate that is indeed a valid dag
        return ValidDag(valid_steps, invalid_steps)

class ValidDag:

    def __init__(self, valid_steps: List[ValidDagStep], invalid_steps: List[InvalidDagStep]):
        self.valid_steps = valid_steps
        self.invalid_steps = invalid_steps

    def to_dict(self) -> dict:
        # TODO: Find Sensible way to display dag
        {}

class InvalidDag:

    def __init__(self, reason: str, valid_steps: List[ValidDagStep], invalid_steps: List[InvalidDagStep]):
        self.valid_steps = valid_steps
        self.invalid_steps = invalid_steps
        invalid_step_reasons: List[str] = list(map(lambda i: i.reason, self.invalid_steps))
        self.reason = reason + " Invalid Dag Steps: " + " ,".join(invalid_step_reasons)

