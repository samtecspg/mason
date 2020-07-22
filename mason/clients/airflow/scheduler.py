from typing import Optional, Tuple

from mason.clients.airflow.airflow_client import AirflowClient
from mason.clients.response import Response
from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.clients.engines.scheduler import SchedulerClient
from mason.clients.local.local_client import LocalClient
from mason.engines.storage.models.path import Path
from mason.util.environment import MasonEnvironment


class AirflowSchedulerClient(SchedulerClient):

    #  This is a local synchronous scheduler.   For asynchronous see AsyncLocal (WIP)
    def __init__(self, config: dict):
        self.client: LocalClient = AirflowClient(config)
        self.dag: Optional[ValidDag] = None

    def register_dag(self, schedule_name: str, valid_dag: ValidDag, response: Response) -> Tuple[str, Response]:
        return self.client.register_dag(schedule_name, valid_dag, response)

    def register_schedule(self, database_name: str, path: Path, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        return self.client.trigger_schedule(schedule_name, response, env)

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

