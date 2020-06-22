from abc import abstractmethod


class WorkflowDefinition:
    from typing import Union

    from mason.engines.scheduler.models.dags.failed_dag_step import FailedDagStep
    from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep
    from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
    from mason.engines.scheduler.models.dags.executed_dag_step import ExecutedDagStep

    @abstractmethod
    def step(self, current: ExecutedDagStep, next: ValidDagStep) -> Union[ValidDagStep, InvalidDagStep, FailedDagStep, ExecutedDagStep]:
        raise NotImplementedError("Step definition not defined")


