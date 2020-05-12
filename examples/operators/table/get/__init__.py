from configurations.valid_config import ValidConfig
from parameters import ValidatedParameters
from clients.response import Response
from api import operator_api as OperatorApi
from util.environment import MasonEnvironment

def api(*args, **kwargs): return OperatorApi.get("table", "get", *args, **kwargs)

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):
    database_name: str = parameters.get_required("database_name")
    table_name: str = parameters.get_required("table_name")

    schema, response = config.metastore.client.get_table(database_name, table_name, response)

    return response

