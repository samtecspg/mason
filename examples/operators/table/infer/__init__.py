
from configurations import Config
from engines.execution.models.jobs.infer_job import InferJob
from parameters import Parameters
from clients.response import Response
from api import operator_api as OperatorApi
from util.environment import MasonEnvironment


def run_new(env: MasonEnvironment, config: Config, parameters: Parameters, response: Response):
    database_name: str = parameters.safe_get("database_name")
    storage_path: str = parameters.safe_get("storage_path")
    
    job = InferJob(database_name, storage_path)


def run(env: MasonEnvironment, config: Config, parameters: Parameters, response: Response):

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

def api(*args, **kwargs): return OperatorApi.get("table", "infer", *args, **kwargs)
