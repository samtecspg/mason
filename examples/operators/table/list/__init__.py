from clients.response import Response
from parameters import ValidatedParameters
from configurations.valid_config import ValidConfig
from util.environment import MasonEnvironment
from api import operator_api as OperatorApi

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):
    database_name: str = parameters.get_required("database_name")
    response = config.metastore.client.list_tables(database_name, response)
    return response

def api(*args, **kwargs): return OperatorApi.get("table", "list", *args, **kwargs)



