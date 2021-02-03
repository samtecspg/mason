from typing import Optional, Tuple, Union

from mason.clients.response import Response
from mason.engines.scheduler.models.dags.client_dag import ClientDag
from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.clients.engines.scheduler import SchedulerClient
from mason.clients.local.local_client import LocalClient
from mason.engines.scheduler.models.schedule import Schedule, InvalidSchedule
from mason.util.environment import MasonEnvironment

class LocalSchedulerClient(SchedulerClient):
    
    #  This is a local synchronous scheduler.   For asynchronous see AsyncLocal (WIP)
    def __init__(self, client: LocalClient):
        self.client = client
        self.dag: Optional[ValidDag] = None
    
    def register_dag(self, schedule_name: str, valid_dag: ValidDag, schedule: Optional[Schedule], response: Response) -> Tuple[str, Response, Optional[ClientDag]]:
        return self.client.register_dag(schedule_name, valid_dag, schedule, response)

    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        return self.client.trigger_schedule(schedule_name, response, env)

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def validate_schedule(self, schedule: Optional[str]) -> Union[Optional[Schedule], InvalidSchedule]:
        # Schedule should not be defined for synchronous local scheduler
        return None

