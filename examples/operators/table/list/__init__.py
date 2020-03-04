from configurations import Config
from parameters import Parameters
from clients.response import Response

def run(config: Config, parameters: Parameters, response: Response):
    database_name: str = parameters.safe_get("database_name")

    response = config.metastore_config.client.list_tables(database_name, response)

    return response

