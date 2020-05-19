from engines.execution.models.jobs.infer_job import InferJob

from configurations.valid_config import ValidConfig
from parameters import ValidatedParameters
from clients.response import Response
from api import operator_api as OperatorApi
from util.environment import MasonEnvironment

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):

    database_name: str = parameters.get_required("database_name")
    storage_path: str = parameters.get_required("storage_path")
    path = config.storage.client.path(storage_path)

    credentials, response = config.metastore.client.credentials(response)
    schemas, response = config.metastore.client.get_table(database_name, storage_path, response)

    if not response.errored():
        job = InferJob(database_name, path, credentials.to_dict())
        response = config.execution.client.run_job(job, response)

    return response

def api(*args, **kwargs): return OperatorApi.get("table", "infer", *args, **kwargs)
