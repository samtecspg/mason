from typing import Optional, Union

from mason.clients.engines.storage import StorageClient
from mason.clients.glue.metastore import GlueMetastoreClient
from mason.engines.metastore.models.table.table import Table
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
                storage_path = storage_engine.get_path(params.get_required("storage_path"))
                if isinstance(storage_path, Path):
                    response = self.register_schedule(db_name, storage_path, schedule_name, schedule, response)
                else:
                    response.add_error(f"Invalid Path: {storage_path.reason}")
            else:
                response = response.add_error(f"Attempted to register_dag for invalid client: {storage_engine.reason}")
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
    def trigger_schedule_for_table(self, table_path: str, response: Response):
        metastore = GlueMetastoreClient(self.client)
        path = metastore.parse_table_path(table_path, "glue")
        if isinstance(path, Path):
            table, response = self.client.get_table(path, response)

            crawler_name = None
            if isinstance(table, Table):
                created_by = table.created_by
                cb = created_by or ""
                if "crawler:" in cb:
                    crawler_name = cb.replace("crawler:", "")
                    self.client.trigger_schedule(crawler_name, response)
                else:
                    response.add_error(f"Table not created by crawler. created_by: {created_by}")
            else:
                response.add_error(f"Could not find table {path.table_name}")
                response.set_status(404)
        else:
            response.add_error(path.reason)
            response.set_status(400)

        return response
