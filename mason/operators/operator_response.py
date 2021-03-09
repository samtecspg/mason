from typing import Optional

from mason.clients.responsable import Responsable
from mason.clients.response import Response

class OperatorResponse:
    
    def __init__(self, resp: Response, object: Optional[Responsable] = None):   #TODO: Generalize object
        self.object = object
        self.response = self.to_response(resp, object)

    def to_response(self, response: Response, object: Optional[Responsable] = None) -> Response:
        if object:
            self.response = object.to_response(response)
        return self.response

    def formatted(self) -> dict:
        return self.response.formatted()
        
    def status_code(self) -> int:
        return self.response.status_code
    
    def with_status(self):
        return self.response.with_status()

