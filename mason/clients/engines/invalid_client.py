
from mason.clients.base import Client
from mason.clients.response import Response

class InvalidClient(Client):

    def __init__(self, reason: str):
        self.reason = reason
        self.client = NullClient(reason)
        
    def __getattr__(self, name):
        def _missing(response: Response, *args, **kwargs) -> Response:
            response.add_error(f"Invalid Client: {self.reason}")
            return response
        return _missing

    def to_dict(self) -> dict:
        return {'name': self.name(), 'error': self.reason}


class NullClient:
    def __init__(self, message: str):
        self.message = message
        
    def name(self):
        return "invalid"
    
    def to_dict(self) -> dict:
        return {
            'error': self.message,
            "name": 'invalid'
        }
    
