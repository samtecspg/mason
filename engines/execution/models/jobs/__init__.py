from typing import List, Optional, Dict

from clients.response import Response


class Job:

    def __init__(self, id: str, logs: Optional[List[str]]=None, errors: Optional[List[str]]=None, results: Optional[List[Dict]]=None):
        self.id = id
        self.logs = logs
        self.errors = errors
        self.results = results

    def running(self, response: Response) -> Response:
        response.add_info(f"Running job id={self.id}")
        self.add_logs(response)
        return response

    def add_data(self, response: Response) -> Response:
        response = self.add_errors(response)
        response = self.add_logs(response)
        response = self.add_results(response)
        return response

    def add_errors(self, response: Response) -> Response:
        if self.errors:
            for e in self.errors:
                response.add_error(e)
        return response

    def add_logs(self, response: Response) -> Response:
        if self.logs:
            response.add_data({"Logs": self.logs})
        return response

    def add_results(self, response: Response) -> Response:
        if self.results:
            response.add_data({"Results": self.results})
        return response

    def add_preview(self, response: Response) -> Response:
        if self.preview:
            response.add_data({"Preview": self.preview})
        return response
