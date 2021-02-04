
from mason.clients.base import Client

class InvalidClient(Client):

    def __init__(self, reason: str):
        self.reason = reason
        self.client = NullClient()

class NullClient:
    def name(self):
        return "invalid"
    
