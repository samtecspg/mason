from typing import Optional, List, Union

from mason.engines.execution.models.jobs.executed_job import ExecutedJob
from mason.engines.execution.models.jobs.invalid_job import InvalidJob
from mason.util.uuid import uuid4

class Job:

    def __init__(self, type: str, parameters: Optional[dict] = {}):
        self.type = type
        self.parameters = parameters
        self.logs: List[Union[str, dict]] = []
        self.set_id()

    def add_log(self, log: Union[str, dict]):
        self.logs.append(log)

    def errored(self, error: Optional[str] = None) -> InvalidJob:
        return InvalidJob(error)

    def running(self, message: Optional[str] = None, past=False) -> ExecutedJob:
        if not past:
            self.add_log(f"Running job id={self.id}")
        return ExecutedJob(message, self.logs)

    def set_id(self, id: Optional[str] = None):
        self.id = id or str(uuid4())
