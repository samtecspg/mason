from typing import Optional, Union


from mason.clients.responsable import Responsable
from mason.clients.response import Response
from mason.engines.execution.models.jobs import InvalidJob, ExecutedJob


class OperatorResponse:
    
    def __init__(self, resp: Response, object: Optional[Responsable] = None):   #TODO: Generalize object
        self.object = object
        response = resp
        if object:
            response = object.to_response(response)
        self.response = response
        

    def to_response(self) -> Response:
        return self.response

    def formatted(self) -> dict:
        return self.response.formatted()
        
    def status_code(self) -> int:
        return self.response.status_code
    
    def with_status(self):
        return self.response.with_status()


class DelayedOperatorResponse(OperatorResponse):
    
    def __init__(self, job: Union[ExecutedJob, InvalidJob], response: Response):
        super().__init__(response, job)