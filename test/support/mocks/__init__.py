import contextlib
from unittest.mock import patch, MagicMock
from test.support.mocks import clients as mock_clients
from test.support.mocks.clients.kubernetes import KubernetesMock

from util.logger import logger


CONFIGURED_MOCKS = {
    "glue": "glue",
    "s3": "s3",
    "spark": "kubernetes",
    "athena": "athena"
}

def method_name(client_name: str) -> str:
    return f"clients.{client_name}.{client_name.capitalize()}Client.client"

def get_patches():
    return list(map(lambda m: patch(method_name(m[0]), get_magic_mock(m[1])), CONFIGURED_MOCKS.items()))

def get_magic_mock(mock_name: str):
    logger.debug(f"Mocking {mock_name} client")
    return MagicMock(return_value=eval(f"mock_clients.{mock_name.capitalize()}Mock()"))
