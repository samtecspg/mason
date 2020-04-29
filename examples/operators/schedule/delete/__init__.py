from configurations import Config
from util.environment import MasonEnvironment
from parameters import Parameters
from clients.response import Response
from api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("schedule", "delete", *args, **kwargs)

def run(env: MasonEnvironment, config: Config, parameters: Parameters, response: Response):
    schedule_name: str = parameters.safe_get("schedule_name")

    response = config.scheduler.client.delete_schedule(schedule_name, response)

    return response



