from typing import Type

from mason.clients.base import Client
from mason.validations.config import ConfigProto as BaseConfigProto

class ConfigProto(BaseConfigProto):

    def supported_client(self, client_name: str) -> Type[Client]:
        if client_name == "test":
            from mason.test.support.clients.test import TestClient
            return TestClient
        elif client_name == "test2":
            from mason.test.support.clients.test2 import Test2Client
            return Test2Client
        elif client_name == "local":
            from mason.clients.local.local_client import LocalClient
            return LocalClient
        else:
            from mason.clients.engines.invalid_client import InvalidClient
            return InvalidClient

    def client_path(self) -> str:
        return "/test/support/clients/"

    def client_module(self) -> str:
        return "mason.test.support.clients"
