from typing import Optional, Tuple

from mason.clients.response import Response
from mason.engines.scheduler.models.dags.client_dag import ClientDag
from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.clients.engines.scheduler import SchedulerClient
from mason.clients.local.local_client import LocalClient
from mason.util.environment import MasonEnvironment


class LocalSchedulerClient(SchedulerClient):
    
    #  This is a local synchronous scheduler.   For asynchronous see AsyncLocal (WIP)
    def __init__(self, config: dict):
        self.client: LocalClient = LocalClient(config)
        self.dag: Optional[ValidDag] = None
    
    def register_dag(self, schedule_name: str, valid_dag: ValidDag, response: Response) -> Tuple[str, Response, Optional[ClientDag]]:
        return self.client.register_dag(schedule_name, valid_dag, response)

    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        return self.client.trigger_schedule(schedule_name, response, env)

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

