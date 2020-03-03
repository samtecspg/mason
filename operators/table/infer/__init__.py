
from configurations import Config
from parameters import Parameters
from configurations.response import Response

def run(config: Config, parameters: Parameters, response: Response):
    database_name: str = parameters.safe_get("database_name")
    storage_path: str = parameters.safe_get("storage_path")
    schedule_name: str = parameters.safe_get("schedule_name")

    config.scheduler_config.client.register_schedule

    storage_client = config.storage_config.client
    metastore_client = config.metastore_config.client

    response = config.scheduler_config.client.register_schedule(metastore_client, storage_client, database_name, storage_path, schedule_name, response)
    response = config.scheduler_config.client.trigger_schedule(schedule_name, response)

    return response

