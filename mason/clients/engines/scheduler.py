from typing import Tuple, Optional, Union
from abc import abstractmethod

from mason.clients.base import Client
from mason.engines.scheduler.models.dags.client_dag import ClientDag
from mason.engines.scheduler.models.schedule import Schedule, InvalidSchedule
from mason.clients.response import Response
from mason.util.environment import MasonEnvironment

class SchedulerClient:
    
    @abstractmethod
    def __init__(self, client: Client):
        self.client = client

    @abstractmethod
    def register_dag(self, schedule_name: str, valid_dag: 'ValidDag', schedule: Optional[Schedule], response: Response) -> Tuple[str, Response, Optional[ClientDag]]: # type: ignore
        raise NotImplementedError("Client method not implemented")

    @abstractmethod
    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        raise NotImplementedError("Client method not implemented")

    @abstractmethod
    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")
    
    @abstractmethod
    def validate_schedule(self, schedule: Optional[str]) -> Union[Optional[Schedule], InvalidSchedule]:
        raise NotImplementedError("Client method not implemented")

    # TODO: Remove
    @abstractmethod
    def trigger_schedule_for_table(self, table_path: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

        

