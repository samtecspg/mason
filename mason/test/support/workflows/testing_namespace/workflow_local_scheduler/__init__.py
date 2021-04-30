from typing import Union

from mason.engines.metastore.models.table.table import Table
from mason.engines.scheduler.models.dags.failed_dag_step import FailedDagStep
from mason.engines.scheduler.models.dags.executed_dag_step import ExecutedDagStep
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
from mason.workflows.workflow_definition import WorkflowDefinition

class TestingNamespaceWorkflowLocalScheduler(WorkflowDefinition):
    def step(self, current: ExecutedDagStep, next: ValidDagStep) -> Union[ValidDagStep, FailedDagStep]:
        if current.step.id == "step_1":
            if isinstance(current.operator_response.object, Table):
                return next
            else:
                return current.failed("Table not found in operator_response")
        else:
            return next
