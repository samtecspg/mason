from typing import Union

from mason.engines.scheduler.models.dags.failed_dag_step import FailedDagStep
from mason.engines.scheduler.models.dags.executed_dag_step import ExecutedDagStep
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep

def step(current: ExecutedDagStep, next: ValidDagStep) -> Union[ValidDagStep, FailedDagStep]:
    return next
    
