import time
from typing import Union, Optional

from mason.engines.scheduler.models.dags.invalid_dag_step import InvalidDagStep
from mason.engines.scheduler.models.dags.valid_dag_step import ValidDagStep
from mason.operators.operator_response import OperatorResponse

class FailedDagStep:
    def __init__(self, reason: str, step: ValidDagStep, response: OperatorResponse):
        self.reason = reason
        self.step = step
        self.operator_response = response # TODO: Make into responses plural

    def retry(self) -> Union[ValidDagStep, InvalidDagStep]:
        retry_method: Optional[str] = self.step.retry_method     
        if retry_method:
            retry_length:int = self.step.retry_delay
            if retry_method is "exponential":
                if self.step.retryable():
                    self.operator_response.response.add_info(f"Retrying step {self.step.id}. Attempt {self.step.retries}")
                    retry_interval = retry_length * (2 ** self.step.retries)
                    time.sleep(retry_interval)
                    self.step.retries += 1
                    return self.step
                else:
                    return InvalidDagStep(f"Step retries ({self.step.retries}) exceeded maximum ({self.step.retry_max})")
            elif retry_method is "constant":
                if self.step.retryable():
                    self.operator_response.response.add_info(f"Retrying step. Attempt {self.step.retries}")
                    time.sleep(retry_length)
                    self.step.retries += 1
                    return self.step
                else:
                    return InvalidDagStep(f"Step retries ({self.step.retries}) exceeded maximum ({self.step.retry_max})")
            else:
                return InvalidDagStep("Unsupported retry method")
        else:
            return InvalidDagStep("'retry' not defined for DagStep")
