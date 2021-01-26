from dataclasses import dataclass

from mason.clients.base import Client

@dataclass
class Test2Client(Client):
    test_param_3: str
    test_param_4: str

