from abc import abstractmethod

from mason.clients.response import Response

class Responsable:
    
    @abstractmethod
    def to_response(self, prior: Response) -> Response:
        raise NotImplementedError("to_response Not Implmeneted for object")
