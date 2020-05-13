from clients.response import Response
from configurations.valid_config import ValidConfig
from parameters import ValidatedParameters
from util.environment import MasonEnvironment

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):
    database_name: str = parameters.get_required("database_name")
    storage_path: str = parameters.get_required("storage_path")
    schedule_name: str = parameters.get_required("schedule_name")
    schedule: str = parameters.get_required("schedule")


    response = config.scheduler

    return response
