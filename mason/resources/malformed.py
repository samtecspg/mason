from dataclasses import dataclass
from typing import Optional

from typistry.protos.invalid_object import InvalidObject

from mason.clients.response import Response
from mason.resources.resource import Resource
from mason.resources.saveable import Saveable
from mason.state.base import MasonStateStore

@dataclass
class MalformedResource(Saveable, Resource):
    invalid_obj: Optional[InvalidObject] = None
    message: Optional[str] = None
    
    def get_message(self) -> Optional[str]:
        if self.invalid_obj:
            return f"Malformed resource: {self.invalid_obj.message}: {self.invalid_obj.reference}"
        elif self.message:
            return self.message
        else:
            return None 

    def ignorable(self) -> bool:
        return (self.get_message() == None)
    
    def save(self, state_store: MasonStateStore, overwrite: bool = False, response: Response = Response()) -> Response:
        message = self.get_message()
        if message:
            response.add_error(message)
        return response
