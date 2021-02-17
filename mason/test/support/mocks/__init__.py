from unittest.mock import patch, MagicMock

from mason.test.support.mocks import clients as mock_clients
from mason.util.string import to_class_case
from mason.util.logger import logger


CONFIGURED_MOCKS = {
    "glue": "glue",
    "s3": "s3",
    "spark": "kubernetes",
    "dask": "kubernetes_worker",
    "athena": "athena"
}

def method_name(client_name: str) -> str:
    return f"mason.clients.{client_name}.{client_name}_client.{client_name.capitalize()}Client.client"

def get_patches():
    return list(map(lambda m: patch(method_name(m[0]), get_magic_mock(m[1])), CONFIGURED_MOCKS.items()))

def get_magic_mock(mock_name: str):
    logger.debug(f"Mocking {mock_name} client")
    return MagicMock(return_value=eval(f"mock_clients.{to_class_case(mock_name)}Mock()"))

