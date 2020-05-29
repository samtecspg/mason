from typing import Optional, List

from mason.clients.response import Response
from mason.util.uuid import uuid4


class Job:

    def __init__(self, type: str, parameters: Optional[dict] = {}, response: Optional[Response] = None):
        self.type = type
        self.parameters = parameters
        self.response = response or Response()
        self.logs: List[str] = []
        self.set_id()

    def add_log(self, log: str):
        self.logs.append(log)
        self.response.add_info(log)

    def add_data(self, data: dict):
        self.response.add_data(data)

    def errored(self, error: str) -> 'InvalidJob':
        self.response.add_error(error)
        return InvalidJob(self, error)

    def running(self, message: Optional[str] = None) -> 'ExecutedJob':
        if message:
            self.response.add_info(message)
        else:
            self.response.add_info(f"Running job id={self.id}")
        return ExecutedJob(self)

    def set_id(self, id: Optional[str] = None):
        self.id = id or str(uuid4())


class ExecutedJob:
    def __init__(self, job: Job):
        self.job = job

class InvalidJob:

    def __init__(self, job: Job, reason: str):
        self.reason = reason
        self.job = job

    def run(self, response: Response):
        response.add_error(f"Invalid Job. Reason: {self.reason}")
