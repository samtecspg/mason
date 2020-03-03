from configurations import Config
from parameters import Parameters
from configurations.response import Response

def run(config: Config, parameters: Parameters, response: Response):
    database_name: str = parameters.safe_get("database_name")
    table_name: str = parameters.safe_get("table_name")

    response = config.metastore_config.client
    response = config.scheduler_client.client.trigger_schedule(table_name, database_name, response)

    print()
    client_name = config.metastore_config.client_name or ""
    print(f"{client_name.capitalize()} Client Response:")
    return response

