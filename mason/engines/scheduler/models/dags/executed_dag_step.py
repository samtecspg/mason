from importlib import import_module
from typing import List, Union

from mason.engines.scheduler.models.dags.failed_dag_step import FailedDagStep
from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep
from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
from mason.operators.operator_response import OperatorResponse
from mason.util.logger import logger
from mason.util.environment import MasonEnvironment


class ExecutedDagStep:
    def __init__(self, step: ValidDagStep, response: OperatorResponse):
        self.step = step
        self.operator_response = response
        
    def next_steps(self, dag: ValidDag, env: MasonEnvironment) -> List[Union[ValidDagStep, InvalidDagStep]]:
        next_steps = dag.get_next_steps(self.step)
        return list(map(lambda s: self.check_step(env, s, dag), next_steps))

    def failed(self, reason: str):
        return FailedDagStep(reason, self.step, self.operator_response)

    def check_step(self, env: MasonEnvironment, step: ValidDagStep, dag: ValidDag) -> Union[ValidDagStep, InvalidDagStep]:
        step_response: Union[ValidDagStep, InvalidDagStep]
        try:
            mod = import_module(f'{env.workflow_module}.{dag.namespace}.{dag.command}')
            stepped: Union[ValidDagStep, FailedDagStep] = mod.step(self, step)
            if isinstance(stepped, FailedDagStep):
                step_response = stepped.retry()
            else:
                step_response = stepped
        except ModuleNotFoundError as e:
            logger.debug(f"step function not defined for {dag.namespace}:{dag.command}")
            step_response = step
            
        return step_response