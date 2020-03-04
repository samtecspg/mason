from configurations import Config
from parameters import Parameters
from clients.response import Response

def run(config: Config, parameters: Parameters, response: Response):
    database_name: str = parameters.safe_get("database_name")
    table_name: str = parameters.safe_get("table_name")

    response = config.scheduler_config.client.trigger_schedule_for_table(table_name, database_name, response)

    return response

