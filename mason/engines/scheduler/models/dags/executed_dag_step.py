from functools import total_ordering
from importlib import import_module
from typing import List, Union

from mason.engines.scheduler.models.dags.failed_dag_step import FailedDagStep
from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
from mason.operators.operator_response import OperatorResponse
from mason.util.logger import logger
from mason.util.environment import MasonEnvironment

class ExecutedDagStep:
    def __init__(self, step: ValidDagStep, response: OperatorResponse):
        self.step = step
        self.operator_response = response

    def next_steps(self, dag: 'ValidDag', env: MasonEnvironment, executed_steps: List['ExecutedDagStep']) -> List[Union[ValidDagStep, InvalidDagStep, 'ExecutedDagStep']]:
        next_steps: List[ValidDagStep]
        next_steps = dag.get_next_steps(self.step)
        
        if not self.step.id in list(map(lambda e: e.id, next_steps)):
            ns = next_steps
        else:
            ns = []
        return list(map(lambda s: self.check_step(env, s, dag, executed_steps), ns))

    def failed(self, reason: str):
        return FailedDagStep(reason, self.step, self.operator_response)

    def check_step(self, env: MasonEnvironment, step: ValidDagStep, dag: 'ValidDag', executed_steps: List['ExecutedDagStep']) -> Union[ValidDagStep, InvalidDagStep, 'ExecutedDagStep']:
        step_response: Union[ValidDagStep, InvalidDagStep]
        try:
            mod = import_module(f'{env.workflow_module}.{dag.namespace}.{dag.command}')
            stepped: Union[ValidDagStep, FailedDagStep] = mod.step(self, step)
            if isinstance(stepped, FailedDagStep):
                step_response = stepped.retry()
            else:
                # check that all dependencies are satisfied, otherwise return the step as pending
                dep = stepped.dependencies
                executed_step_ids = list(map(lambda e: e.step.id,executed_steps))
                unsatisfied_steps = set(dep).difference(set(executed_step_ids))
                if len(unsatisfied_steps) > 0:
                    return self
                else:
                    return stepped
                    
                    
        except ModuleNotFoundError as e:
            logger.debug(f"step function not defined for {dag.namespace}:{dag.command}")
            step_response = step
            
        return step_response