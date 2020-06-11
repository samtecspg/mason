from typing import List, Union, Set

from mason.engines.scheduler.models.dags.dag_step import DagStep, InvalidDagStep, ValidDagStep
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.util.environment import MasonEnvironment

from mason.util.list import flatten, sequence, get


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
            return self.validate_is_dag(valid_steps, invalid_steps) 
        
    def validate_is_dag(self, valid_steps: List[ValidDagStep], invalid_steps: List[InvalidDagStep]) -> Union['ValidDag', 'InvalidDag']:
        roots = [v for v in valid_steps if len(v.dependencies) == 0]
        is_dag = self.validate_steps(roots, [], valid_steps)
        if isinstance(is_dag, str):
            return InvalidDag(f"Invalid Dag: {is_dag}", valid_steps, invalid_steps)
        else:
            return ValidDag(valid_steps, invalid_steps)

    # n^2 complexity algorithm for detecting cycles
    def validate_steps(self, candidate_steps: List[ValidDagStep], validated_steps: List[ValidDagStep], all_steps: List[ValidDagStep]) -> Union[bool, str]:
        validated_step_ids: List[str] = list(map(lambda s: s.id, validated_steps))
        all_step_ids: List[str] = list(map(lambda s: s.id, all_steps))

        if len(candidate_steps) > 0:
            candidate_step_ids: List[str] = list(map(lambda s: s.id, candidate_steps))
            dependents = [v for v in all_steps if len(set(v.dependencies).intersection(candidate_step_ids)) > 0]
            dependent_step_ids: List[str] = list(map(lambda s: s.id, dependents))
            
            # check for cycles
            repeated = set(dependent_step_ids).intersection(validated_step_ids)
            if len(repeated) > 0:
                return f" Cycle detected. Repeated steps: {repeated}"
            else:
                validated_steps += candidate_steps
                return self.validate_steps(dependents, validated_steps, all_steps)
        else:
            validated = set(validated_step_ids)
            all_steps_ids_set = set(all_step_ids)
            diff = all_steps_ids_set.difference(validated)
            
            if len(diff) == 0:
                return True
            else:
                return f"Unreachable steps: {diff}"

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

