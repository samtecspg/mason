
from clients.engines.scheduler import SchedulerClient
from clients.response import Response
from clients.glue import GlueClient

class GlueSchedulerClient(SchedulerClient):

    def __init__(self, config: dict):
        self.region = config.get("region")
        self.aws_role_arn = config.get("aws_role_arn")
        self.client: GlueClient = GlueClient(self.get_config())

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
            'region': self.region,
            'aws_role_arn': self.aws_role_arn
        }
