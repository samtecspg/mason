from typing import Union

from mason.clients.engines.execution import ExecutionClient, InvalidExecutionClient
from mason.clients.engines.valid_client import EmptyClient
from mason.engines.engine import Engine
from mason.util.logger import logger


class ExecutionEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("execution", config)
        self.client: Union[ExecutionClient, InvalidExecutionClient, EmptyClient] = self.get_client()

    def get_client(self):

        client = self.validate()
        from mason.clients.engines.valid_client import ValidClient
        if isinstance(client, ValidClient):
            if client.client_name == "spark":
                from mason.clients.spark.execution import SparkExecutionClient
                return SparkExecutionClient(client.config)
            elif client.client_name == "athena":
                from mason.clients.athena.execution import AthenaExecutionClient
                return AthenaExecutionClient(client.config)
            elif client.client_name == "dask":
                from mason.clients.dask.execution import DaskExecutionClient
                return DaskExecutionClient(client.config)
            elif client.client_name == "local":
                from mason.clients.local.execution import LocalExecutionClient
                return LocalExecutionClient(client.config)
            else:
                return InvalidExecutionClient(f"Client type not supported {client.client_name}")
        elif isinstance(client, EmptyClient):
            return client
        else:
            return InvalidExecutionClient(client.reason)

    def set_mode(self, mode: str):
        if mode.lower() == "async":
            self.mode = "async"
        elif mode.lower() == "sync":
            self.mode = "sync"
        else:
            logger.error(f"Invalid Mode: {mode}")
            pass
