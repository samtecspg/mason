from typing import List, Union, Optional

from mason.clients.responsable import Responsable
from mason.clients.response import Response

class ExecutedJob(Responsable):
    def __init__(self, id: str, message: Optional[str] = None, logs: Optional[Union[List[Union[str, dict]], List[str]]] = None):
        self.id = id
        self.message = message or ""
        default: List[str] = []
        self.logs: Union[List[Union[str, dict]], List[str]] = logs or default
    
    def to_response(self, response: Response) -> Response:
        if self.message != "":
            response.add_info(self.message)
        if len(self.logs) > 0:
            response.add_data({"Logs": self.logs})
            
        return response
