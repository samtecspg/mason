from typing import Union

from mason.engines.engine import Engine

from mason.clients.engines.scheduler import InvalidSchedulerClient, SchedulerClient
from mason.clients.engines.valid_client import ValidClient, EmptyClient


class SchedulerEngine(Engine):

    def __init__(self, config: dict):
        super().__init__("scheduler", config)
        self.client: Union[SchedulerClient, InvalidSchedulerClient, EmptyClient] = self.get_client()
    
    def get_client(self) -> Union[SchedulerClient, InvalidSchedulerClient, EmptyClient]:
        client = self.validate()
        if isinstance(client, ValidClient):
            if client.client_name == "glue":
                from mason.clients.glue.scheduler import GlueSchedulerClient
                return GlueSchedulerClient(client.config)
            elif client.client_name == "local":
                from mason.clients.local.scheduler import LocalSchedulerClient
                return LocalSchedulerClient(client.config)
            elif client.client_name == "airflow":
                from mason.clients.airflow.scheduler import AirflowSchedulerClient
                return AirflowSchedulerClient(client.config)
            else:
                return InvalidSchedulerClient(f"Client type not supported: {client.client_name}")
        elif isinstance(client, EmptyClient):
            return client
        else:
            return InvalidSchedulerClient(f"{client.reason}")

