from typing import List, Union, Optional

from mason.clients.response import Response
from mason.engines.execution.models.jobs import Job


class GenericJob(Job):
    def __init__(self, id: str, message: Optional[str] = None, logs: Optional[List[Union[str, dict]]] = None):
        self.id = id
        self.message = message or ""
        self.logs = logs or []
        
    def spec(self) -> dict:
        return {}

    def to_response(self, response: Response) -> Response:
        if self.message != "":
            response.add_info(self.message)
            
        if len(self.logs) > 0:
            response.add_data({"Logs": self.logs})

        return response
