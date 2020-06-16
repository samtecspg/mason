from typing import Optional, Any

from mason.clients.responsable import Responsable
from mason.clients.response import Response

class OperatorResponse:
    
    def __init__(self, response: Response, object: Optional[Responsable] = None):   #TODO: Generalize object
        self.object = object
        self.response = self.to_response(response, object)

    def to_response(self, response: Response, object: Optional[Responsable] = None) -> Response:
        if object:
            response = object.to_response(response)
        return response

    def formatted(self) -> dict:
        return self.response.formatted()
        
    def status_code(self) -> int:
        return self.response.status_code
    
    def with_status(self):
        return self.response.with_status()
