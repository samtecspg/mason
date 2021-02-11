from typing import Optional, Union

from mason.clients.engines.storage import StorageClient
from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.engines.scheduler.models.schedule import Schedule, InvalidSchedule
from mason.util.environment import MasonEnvironment

from mason.clients.engines.scheduler import SchedulerClient
from mason.clients.glue.glue_client import GlueClient
from mason.clients.response import Response
from mason.engines.storage.models.path import Path


class GlueSchedulerClient(SchedulerClient):

    def __init__(self, client: GlueClient):
        self.client: GlueClient = client

    def register_dag(self, schedule_name: str, valid_dag: ValidDag, schedule: Optional[Schedule], response: Response):
        #  Short-circuit for glue crawler definition since glue as a scheduler is only well defined for Table Infer Operator
        if len(valid_dag.valid_steps) == 1 and valid_dag.valid_steps[0].operator.type_name() == "TableInfer":
            op = valid_dag.valid_steps[0].operator
            params = valid_dag.valid_steps[0].operator.parameters
            db_name = params.get_required("database_name")

            storage_engine = op.config.storage()
            if isinstance(storage_engine, StorageClient):
                storage_path = storage_engine.path(params.get_required("storage_path"))
            else:
                response = response.add_error(f"Attempted to register_dag for invalid client: {storage_engine.reason}")
            response = self.register_schedule(db_name, storage_path, schedule_name, schedule, response)
        else:
            response.add_error("Glue Scheduler only defined for TableInfer type which registers a glue crawler")

        return (schedule_name, response, None)

    def register_schedule(self, database_name: str, path: Path, schedule_name: str, schedule: Optional[Schedule], response: Response) -> Response:
        response = self.client.register_schedule(database_name, path, schedule_name, schedule, response)
        return response

    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        response = self.client.trigger_schedule(schedule_name, response)
        return response

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        response = self.client.delete_schedule(schedule_name, response)
        return response

    def validate_schedule(self, schedule: Optional[str]) -> Union[Optional[Schedule], InvalidSchedule]:
        if schedule:
            return Schedule(f"cron({schedule})")
        else:
            return None

    # TODO: Remove
    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        response = self.client.trigger_schedule_for_table(table_name, database_name, response)
        return response

