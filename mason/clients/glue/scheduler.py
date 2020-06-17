from mason.engines.scheduler.models.dags.valid_dag import ValidDag
from mason.util.environment import MasonEnvironment

from mason.clients.engines.scheduler import SchedulerClient
from mason.clients.glue.glue_client import GlueClient
from mason.clients.response import Response
from mason.engines.storage.models.path import Path


class GlueSchedulerClient(SchedulerClient):

    def __init__(self, config: dict):
        self.client: GlueClient = GlueClient(config)

    def register_dag(self, schedule_name: str, valid_dag: ValidDag, response: Response):
        #  Short-circuit for glue crawler definition since glue as a scheduler is only well defined for Table Infer Operator
        if len(valid_dag.valid_steps) == 1 and valid_dag.valid_steps[0].operator.type_name() == "TableInfer":
            op = valid_dag.valid_steps[0].operator
            params = valid_dag.valid_steps[0].operator.parameters
            db_name = params.get_required("database_name")

            storage_path = op.config.storage.client.path(params.get_required("storage_path"))
            response = self.register_schedule(db_name, storage_path, schedule_name, response)
        else:
            response.add_error("Glue Scheduler only defined for InferJob type which registers a glue crawler")

        return schedule_name, response

    def register_schedule(self, database_name: str, path: Path, schedule_name: str, response: Response) -> Response:
        response = self.client.register_schedule(database_name, path, schedule_name, response)
        return response

    def trigger_schedule(self, schedule_name: str, response: Response, env: MasonEnvironment) -> Response:
        response = self.client.trigger_schedule(schedule_name, response)
        return response

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        response = self.client.delete_schedule(schedule_name, response)
        return response

    # TODO: Remove
    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        response = self.client.trigger_schedule_for_table(table_name, database_name, response)
        return response

