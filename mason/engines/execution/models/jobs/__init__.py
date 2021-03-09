from typing import Optional, List, Union

from mason.clients.responsable import Responsable
from mason.engines.execution.models.jobs.executed_job import ExecutedJob
from mason.engines.execution.models.jobs.invalid_job import InvalidJob, RetryableJob, FailedJob
from mason.util.uuid import uuid4

class Job:

    def __init__(self, type: str, parameters: Optional[dict] = {}):
        self.type = type
        self.parameters = parameters
        self.logs: List[Union[str, dict]] = []
        self.set_id()

    def add_log(self, log: Union[str, dict]):
        self.logs.append(log)

    def errored(self, error: Optional[str] = None, retryable: Optional[bool] = None) -> InvalidJob:
        r = retryable or False
        if r:
            return RetryableJob(error)
        else:
            return FailedJob(error)

    def running(self, message: Optional[str] = None, past=False, object: Optional[Responsable] = None) -> ExecutedJob:
        if not past:
            self.add_log(f"Running job id={self.id}")
        return ExecutedJob(self.id, message, self.logs, object)

    def set_id(self, id: Optional[str] = None):
        self.id = id or str(uuid4())

