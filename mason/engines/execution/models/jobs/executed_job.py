from typing import List, Union, Optional

from mason.clients.responsable import Responsable
from mason.clients.response import Response

class ExecutedJob(Responsable):
    def __init__(self, id: str, message: Optional[str] = None, logs: Optional[List[Union[str, dict]]] = None, object: Optional[Responsable] = None):
        self.id = id
        self.message = message or ""
        self.logs = logs or []
        self.object: Optional[Responsable] = object
    
    def to_response(self, response: Response) -> Response:
        if self.message != "":
            response.add_info(self.message)
        for l in self.logs:
            if isinstance(l, str):
                response.add_info(l)
            else:
                response.add_data(l)
                
        if self.object:
            response = self.object.to_response(response)
            
        return response
