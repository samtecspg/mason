from typing import Optional

from mason.clients.responsable import Responsable
from mason.clients.response import Response


class InvalidJob(Responsable):

    def __init__(self, reason: Optional[str] = None):
        self.reason = reason
        
    def to_response(self, response: Response):
        if self.reason:
            response.add_error("Job errored: " + self.reason)
        return response

    def run(self):
        pass

