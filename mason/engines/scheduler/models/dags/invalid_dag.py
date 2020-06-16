from typing import List

from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep


class InvalidDag:

    def __init__(self, reason: str, valid_steps: List[ValidDagStep], invalid_steps: List[InvalidDagStep]):
        self.valid_steps = valid_steps
        self.invalid_steps = invalid_steps
        invalid_step_reasons: List[str] = list(map(lambda i: i.reason, self.invalid_steps))
        self.reason = reason + " Invalid Dag Steps: " + " ,".join(invalid_step_reasons)