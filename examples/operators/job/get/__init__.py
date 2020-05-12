from configurations.valid_config import ValidConfig
from parameters import ValidatedParameters
from clients.response import Response
from api import operator_api as OperatorApi
from util.environment import MasonEnvironment

def api(*args, **kwargs): return OperatorApi.get("job", "get", *args, **kwargs)

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):
    job_id: str = parameters.get_required("job_id")
    response = config.execution.client.get_job(job_id, response)

    return response
