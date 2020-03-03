
from configurations import Config
from parameters import Parameters
from clients.response import Response

def run(config: Config, parameters: Parameters, response: Response):
    database_name: str = parameters.safe_get("database_name")
    storage_path: str = parameters.safe_get("storage_path")
    schedule_name: str = parameters.safe_get("schedule_name")

    config.scheduler_config.client.register_schedule


    path = config.storage_config.client.path(storage_path)
    response = config.scheduler_config.client.register_schedule(database_name, path, schedule_name, response)

    if response.status_code == 201:
        response = config.scheduler_config.client.trigger_schedule(schedule_name, response)
    else:
        pass


    return response

