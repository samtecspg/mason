from configurations import Config
from util.environment import MasonEnvironment
from parameters import Parameters
from clients.response import Response
from api import operator_api as OperatorApi
from engines.metastore.models.credentials import MetastoreCredentials

def api(*args, **kwargs): return OperatorApi.get("table", "merge", *args, **kwargs)

def run(env: MasonEnvironment, config: Config, parameters: Parameters, response: Response):
    response = Response()

    #  TODO: Make this consistent, use metastore database and table terminology, verify that the metastore table exists before sending to execution engine
    metastore_client = config.metastore.client
    metastore_credentials: MetastoreCredentials = metastore_client.credentials()

    input_path = metastore_client.full_path(parameters.safe_get("input_path"))
    output_path = metastore_client.full_path(parameters.safe_get("output_path"))
    p = {'input_path': input_path, 'output_path': output_path}
    response = config.execution.client.run_job("merge", metastore_credentials, p, response)

    return response


