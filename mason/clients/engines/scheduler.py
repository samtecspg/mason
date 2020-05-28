from typing import Tuple
from abc import abstractmethod

from mason.clients.client import Client
from mason.clients.response import Response
from mason.engines.storage.models.path import Path


class SchedulerClient(Client):

    @abstractmethod
    def register_dag(self, schedule_name: str, valid_dag, response: Response) -> Tuple[str, Response]:
        raise NotImplementedError("Client method not implemented")
        return response

    @abstractmethod
    def register_schedule(self, database_name: str, path: Path, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")
        return response

    @abstractmethod
    def trigger_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")
        return response

    @abstractmethod
    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")
        return response

    # TODO: Remove
    @abstractmethod
    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")
        return response


