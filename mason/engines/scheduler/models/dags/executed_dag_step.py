import time
from functools import total_ordering
from importlib import import_module
from typing import List, Union

from mason.engines.scheduler.models.dags.failed_dag_step import FailedDagStep
from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep
from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
from mason.operators.operator_response import OperatorResponse
from mason.util.environment import MasonEnvironment
from mason.util.string import to_class_case
from mason.workflows.invalid_workflow import InvalidWorkflow


@total_ordering
class ExecutedDagStep:
    def __init__(self, step: ValidDagStep, response: OperatorResponse):
        self.step = step
        self.operator_response = response
        self.runtime = time.time()

    def __ge__(self, other: 'ExecutedDagStep'):
        return self.runtime > other.runtime

    def __le__(self, other: 'ExecutedDagStep'):
        return not self.__ge__(other)

    def failed(self, reason: str):
        return FailedDagStep(reason, self.step, self.operator_response)

