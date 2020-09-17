from typing import Union

from mason.engines.scheduler.models.dags.executed_dag_step import ExecutedDagStep
from mason.engines.scheduler.models.dags.failed_dag_step import FailedDagStep
from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
from mason.workflows.workflow_definition import WorkflowDefinition


class TableExport(WorkflowDefinition):
    
    def step(self, current: ExecutedDagStep, next: ValidDagStep) -> Union[ValidDagStep, InvalidDagStep, FailedDagStep, ExecutedDagStep]:
        return next
