from typing import Optional, Tuple, Union

from mason.clients.airflow.airflow_client import AirflowClient
from mason.clients.airflow.airflow_dag import AirflowDag
from mason.clients.response import Response
from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.clients.engines.scheduler import SchedulerClient
from mason.engines.scheduler.models.schedule import Schedule, InvalidSchedule
from mason.engines.storage.models.path import Path
from mason.util.environment import MasonEnvironment

class AirflowSchedulerClient(SchedulerClient):

    def __init__(self, config: dict):
        self.client: AirflowClient = AirflowClient(config)
        self.dag: Optional[ValidDag] = None

    def register_dag(self, schedule_name: str, valid_dag: ValidDag, schedule: Optional[Schedule], response: Response) -> Tuple[str, Response, Optional[AirflowDag]]:
        return self.client.register_dag(schedule_name, valid_dag, schedule, response)

    def register_schedule(self, database_name: str, path: Path, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        return self.client.trigger_schedule(schedule_name, response, env)

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        raise NotImplementedError("Client method not implemented")

    def validate_schedule(self, schedule: Optional[str]) -> Union[Optional[Schedule], InvalidSchedule]:
        # TODO:
        return None


