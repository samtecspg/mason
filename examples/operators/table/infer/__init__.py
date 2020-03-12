
from configurations import Config
from parameters import Parameters
from clients.response import Response
import operators.operators as Operator
from util.environment import MasonEnvironment

def run(config: Config, parameters: Parameters, response: Response):
    database_name: str = parameters.safe_get("database_name")
    storage_path: str = parameters.safe_get("storage_path")
    schedule_name: str = parameters.safe_get("schedule_name")

    path = config.storage.client.path(storage_path)
    response = config.scheduler.client.register_schedule(database_name, path, schedule_name, response)

    if response.status_code == 201:
        response = config.scheduler.client.trigger_schedule(schedule_name, response)
    else:
        pass


    return response

# TODO: Automate this
def api(schedule_name: str, database_name: str, storage_path: str):
    env = MasonEnvironment()
    config = Config(env)
    parameters = f"schedule_name:{schedule_name},database_name:{database_name},storage_path:{storage_path}"
    params = Parameters(parameters)
    response = Operator.run(config, params, "table", "infer")

    return response.formatted(), response.status_code

