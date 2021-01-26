from dataclasses import dataclass
from mason.clients.base import Client

@dataclass
class TestClient(Client):
    test_param_1: str
    test_param_2: str
    
