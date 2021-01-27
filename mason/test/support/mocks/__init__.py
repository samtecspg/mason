from typing import Optional, Union
from unittest.mock import patch, MagicMock

from mason.clients.engines.invalid_client import InvalidClient
from mason.definitions import from_root
from mason.engines.scheduler.models.schedule import Schedule, InvalidSchedule
from mason.test.support.clients.test import TestClient
from mason.test.support.clients.test2 import Test2Client
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


def mock_metastore_engine_client(self):
    return mock_engine_client("metastore", self)

def mock_scheduler_engine_client(self):
    return mock_engine_client("scheduler", self)

def mock_execution_engine_client(self):
    return mock_engine_client("execution", self)

def mock_storage_engine_client(self):
    return mock_engine_client("storage", self)

def mock_engine_client(engine_type: str, self):
    if engine_type == "metastore":
        if self.client_name  == "test":
            return TestClient()
        else:
            if self.client_name:
                return InvalidClient(f"Client type not supported: {self.client_name}")
            else:
                return EmptyClient()
    elif engine_type == "execution":
        if self.client_name == "test2":
            return Test2Client()
        else:
            if self.client_name:
                return InvalidClient(f"Client type not supported: {self.client_name}")
            else:
                return EmptyClient()
    elif engine_type == "scheduler":
        if self.client_name == "test2":
            return Test2Client()
        else:
            if self.client_name:
                return InvalidClient(f"Client type not supported: {self.client_name}")
            else:
                return EmptyClient()
    elif engine_type == "storage":
        if self.client_name == "test":
            return TestClient()
        else:
            if self.client_name:
                return InvalidClient(f"Client type not supported: {self.client_name}")
            else:
                return EmptyClient()
    else:
        return InvalidClient("Unsupported Engine")
    
def mock_config_schema(self):
    return from_root("/test/support/configs/schema.json")
