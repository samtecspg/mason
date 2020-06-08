from typing import Optional, Any

from mason.clients.responsable import Responsable
from mason.clients.response import Response

class OperatorResponse:
    
    def __init__(self, response: Response, objects: Optional[Responsable] = None): 
        self.objects = objects
        self.response = self.merge_response(response, objects)

    def merge_response(self, response: Response, object: Optional[Responsable]) -> Response:
        if object:
            response = object.to_response(response)
        return response

    def formatted(self) -> dict:
        return self.response.formatted()
        
    def status_code(self) -> int:
        return self.response.status_code
    
    def with_status(self):
        return self.response.with_status()
