from typing import Tuple

from clients.response import Response
from clients import Client
from abc import abstractmethod


class SchedulerClient(Client):

    ###  IMPORTANT:   This ensures that implemented specific metastore client implementations conform to the needed template when 'mypy .' is run
    ###  which will return Cannot instantiate abstract class 'GlueSchedulerClient' with abstract attribute 'register_schedule' (for example)

    @abstractmethod
    def register_dag(self, schedule_name: str, valid_dag, response: Response) -> Tuple[str, Response]:
        raise NotImplementedError("Client method not implemented")
        return response

    @abstractmethod
    def register_schedule(self, database_name: str, path: str, schedule_name: str, response: Response) -> Response:
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


class EmptySchedulerClient(SchedulerClient):

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")
        return response

    def register_schedule(self, database_name: str, path: str, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")
        return response

    def trigger_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")
        return response
        # return self.config.scheduler.trigger_schedule(schedule_name, response)

    # TODO: Remove
    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")
        return response

