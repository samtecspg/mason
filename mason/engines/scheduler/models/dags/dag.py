from typing import List, Union, Set, Tuple

from mason.engines.scheduler.models.dags.dag_step import DagStep

from mason.engines.scheduler.models.dags.invalid_dag import InvalidDag
from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep
from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.util.environment import MasonEnvironment
from mason.util.list import flatten, sequence


class Dag:

    def __init__(self, namespace: str, command: str, dag_config: List[dict]):
        self.namespace = namespace
        self.command = command
        self.steps = flatten(list(map(lambda step: DagStep(step), dag_config)))

    def validate(self, env: MasonEnvironment, all_parameters: WorkflowParameters) -> Union[ValidDag, InvalidDag]:
        all_steps = self.steps
        all_step_ids = list(map(lambda s: s.id,all_steps))
        validated: List[Union[ValidDagStep, InvalidDagStep]] = flatten(list(map(lambda s: s.validate(env, all_parameters, all_step_ids), self.steps)))
        valid_steps, invalid_steps = sequence(validated, ValidDagStep, InvalidDagStep)
        
        roots: List[ValidDagStep] = [v for v in valid_steps if len(v.dependencies) == 0]
        return self.validate_dag(valid_steps, invalid_steps, roots)

    def validate_dag(self, valid_steps: List[ValidDagStep], invalid_steps: List[InvalidDagStep], roots: List[ValidDagStep]) -> Union[ValidDag, InvalidDag]:
        if len(valid_steps) == 0:
            return InvalidDag("No valid DAG steps. ", valid_steps, invalid_steps)
        else:
            is_dag = self.validate_steps(set(roots), set(valid_steps))
            if isinstance(is_dag, str):
                return InvalidDag(f"Invalid Dag: {is_dag}", valid_steps, invalid_steps)
            else:
                return ValidDag(self.namespace, self.command, valid_steps, invalid_steps)

    def validate_steps(self, roots: Set[ValidDagStep], valid_steps: Set[ValidDagStep]) -> Union[bool, str]:
        visited: Set[ValidDagStep] = set()
        stack: Set[ValidDagStep] = set()
        for step in roots:
            if not step in visited:
                test, visited, stack = self.is_cyclic(step, visited, stack, valid_steps)
                if isinstance(test, str):
                    return test
                else:
                    return False
            else:
                return False
        
        diff = valid_steps.difference(visited)
        if len(diff) > 0:
            diff_ids = list(map(lambda s: s.id, diff))
            return f"Unreachable steps: {diff_ids}"
        else:
            return False

    def is_cyclic(self, step: ValidDagStep, visited: Set[ValidDagStep], stack: Set[ValidDagStep], valid_steps: Set[ValidDagStep]) -> Tuple[Union[bool, str], Set[ValidDagStep], Set[ValidDagStep]]:
        visited.add(step)
        stack.add(step)
        children = list(filter(lambda s: step.id in s.dependencies, valid_steps))
        for dep in children:
            if dep not in visited:
                next, visited, stack = self.is_cyclic(dep, visited, stack, valid_steps)
                if isinstance(next, str):
                    return next, visited, stack
            elif dep in stack:
                return f"Cycle detected. Repeated steps: {dep.id}", visited, stack
        stack.remove(step)
        return False, visited, stack

