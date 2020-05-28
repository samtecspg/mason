from engines.execution.models.jobs import ExecutedJob

from configurations.valid_config import ValidConfig
from parameters import ValidatedParameters
from clients.response import Response
from api import operator_api as OperatorApi
from util.environment import MasonEnvironment

def api(*args, **kwargs): return OperatorApi.get("job", "get", *args, **kwargs)

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):
    job_id: str = parameters.get_required("job_id")
    executed = config.execution.client.get_job(job_id, response)

    if isinstance(executed, ExecutedJob):
        response = executed.job.response
    else:
        response.add_error(executed.reason)

    return response

