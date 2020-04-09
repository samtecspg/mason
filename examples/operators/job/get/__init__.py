from configurations import Config
from parameters import Parameters
from clients.response import Response
from api import operator_api as OperatorApi
from util.environment import MasonEnvironment

def api(*args, **kwargs): return OperatorApi.get("job", "get", *args, **kwargs)

def run(env: MasonEnvironment, config: Config, parameters: Parameters, response: Response):
    job_id: str = parameters.safe_get("job_id")
    response = config.execution.client.get_job(job_id, response)

    return response
