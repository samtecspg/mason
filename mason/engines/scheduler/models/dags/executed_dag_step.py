import time
from functools import total_ordering

from mason.engines.scheduler.models.dags.failed_dag_step import FailedDagStep
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
from mason.operators.operator_response import OperatorResponse


@total_ordering
class ExecutedDagStep:
    def __init__(self, step: ValidDagStep, response: OperatorResponse):
        self.step = step
        self.operator_response = response
        self.runtime = time.time()

    def __gt__(self, other: 'ExecutedDagStep'):
        return self.runtime > other.runtime

    def __lt__(self, other: 'ExecutedDagStep'):
        return not self.__gt__(other)

    def failed(self, reason: str):
        return FailedDagStep(reason, self.step, self.operator_response)

