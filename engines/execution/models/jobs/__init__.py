from typing import List, Dict

from clients.response import Response

class Job:

    def __init__(self, type: str, parameters: dict):
        self.type = type
        self.parameters = parameters


class ExecutedJob:
    def __init__(self, id: str, logs: List[str] = [], errors: List[str]=[], results: List[str]=[]):
        self.id = id
        self.logs = logs
        self.errors = errors
        self.results = results

    def to_response(self, response: Response) -> Response:

        for e in self.errors:
            response.add_error(e)

        if len(self.logs) > 0:
            response.add_data({"Logs": self.logs})

        if len(self.results) > 0:
            response.add_data({"Results": self.results})

        return response


class InvalidJob:

    def __init__(self, reason: str):
        self.reason = reason

    def run(self, response: Response):
        response.add_error(f"Invalid Job. Reason: {response}")
