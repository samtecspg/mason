from mason.test.support.clients.test import TestClient
from mason.test.support.clients.test2 import Test2Client
from mason.validations.config import ConfigProto

class TestConfigProto(ConfigProto):
    
    def supported_clients(self) -> dict:
        return {
            "test": TestClient,
            "test2": Test2Client
        }

    def client_path(self) -> str:
        return "/test/support/clients/"

