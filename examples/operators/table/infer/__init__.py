
from configurations.valid_config import ValidConfig
from engines.execution.models.jobs.infer_job import InferJob
from parameters import ValidatedParameters
from clients.response import Response
from api import operator_api as OperatorApi
from util.environment import MasonEnvironment

def job(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters) -> InferJob:
    database_name: str = parameters.get_required("database_name")
    storage_path: str = parameters.get_required("storage_path")
    path = config.storage.client.path(storage_path)

    job = InferJob(database_name, path)
    return job

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):
    job(env, config, parameters).run(response)

    return response

def api(*args, **kwargs): return OperatorApi.get("table", "infer", *args, **kwargs)
