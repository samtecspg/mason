from clients.engines.scheduler import SchedulerClient
from clients.response import Response
from clients.aws.glue import GlueClient

class GlueSchedulerClient(SchedulerClient):

    def __init__(self, config: dict):
        self.aws_region = config.get("aws_region")
        self.aws_role_arn = config.get("aws_role_arn")
        self.access_key = config.get("access_key")
        self.secret_key = config.get("secret_key")
        self.client: GlueClient = GlueClient(self.get_config())

    # def register_dag(self, schedule_name: str, valid_dag, response: Response):
    #     #  Short-circuit for glue crawler definition since glue as a scheduler is only well defined for Table Infer Operator
    #     if len(valid_dag.valid_steps) == 1 and valid_dag.valid_steps[0].operator.type_name() == "TableInfer":
    #         response = self.register_schedule(,,, schedule_name, response)
    #     else:
    #         response.add_error("Glue Scheduler only defined for InferJob type which registers a glue crawler")
    #
    #     return schedule_name, response

    def register_schedule(self, database_name: str, path: str, schedule_name: str, response: Response) -> Response:
        response = self.client.register_schedule(database_name, path, schedule_name, response)
        return response

    def trigger_schedule(self, schedule_name: str, response: Response) -> Response:
        response = self.client.trigger_schedule(schedule_name, response)
        return response

    def delete_schedule(self, schedule_name: str, response: Response) -> Response:
        response = self.client.delete_schedule(schedule_name, response)
        return response

    # TODO: Remove
    def trigger_schedule_for_table(self, table_name: str, database_name: str, response: Response) -> Response:
        response = self.client.trigger_schedule_for_table(table_name, database_name, response)
        return response

    def get_config(self):
        return {
            'aws_region': self.aws_region,
            'aws_role_arn': self.aws_role_arn,
            'access_key': self.access_key,
            'secret_key': self.secret_key
        }

