from typing import Tuple, Optional, Union
from abc import abstractmethod

from mason.engines.scheduler.models.dags.client_dag import ClientDag
from mason.engines.scheduler.models.schedule import Schedule, InvalidSchedule
from mason.util.environment import MasonEnvironment

from mason.clients.client import Client
from mason.clients.engines.invalid_client import InvalidClient
from mason.clients.response import Response
from mason.engines.storage.models.path import Path


class SchedulerClient(Client):

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
    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

        

class InvalidSchedulerClient(SchedulerClient, InvalidClient):
    
    def __init__(self, reason: str):
        super().__init__(reason)

    def register_dag(self, schedule_name: str, valid_dag: 'ValidDag', schedule: Optional[Schedule], response: Response) -> Tuple[str, Response, Optional[ClientDag]]: # type: ignore
        raise NotImplementedError("Client method not implemented")

    def register_schedule(self, database_name: str, path: Path, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        raise NotImplementedError("Client method not implemented")

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def validate_schedule(self, schedule: Optional[str]) -> Union[Optional[Schedule], InvalidSchedule]:
        raise NotImplementedError("Client method not implemented")

    # TODO: Remove
    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")


