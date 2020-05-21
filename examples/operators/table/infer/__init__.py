from engines.metastore.models.credentials import MetastoreCredentials

from engines.execution.models.jobs.infer_job import InferJob

from configurations.valid_config import ValidConfig
from engines.metastore.models.database import Database
from parameters import ValidatedParameters
from clients.response import Response
from api import operator_api as OperatorApi
from util.environment import MasonEnvironment

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):

    database_name: str = parameters.get_required("database_name")
    storage_path: str = parameters.get_required("storage_path")

    database = config.metastore.client.get_database(database_name)
    path = config.storage.client.get_path(storage_path)

    if isinstance(database, Database):
        credentials, response = config.metastore.client.credentials()
        if isinstance(credentials, MetastoreCredentials):
            job = InferJob(database, path, credentials)
        else:
            response.add_error(credentials.reason)
    else:
        response.add_error(f"Invalid Database: {database.reason}")

    return response

def api(*args, **kwargs): return OperatorApi.get("table", "infer", *args, **kwargs)
