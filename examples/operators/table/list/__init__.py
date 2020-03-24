from clients.response import Response
from parameters import Parameters
from configurations import Config
from util.environment import MasonEnvironment
from api import operator_api as OperatorApi

def run(env: MasonEnvironment, config: Config, parameters: Parameters, response: Response):
    database_name: str = parameters.safe_get("database_name")
    response = config.metastore.client.list_tables(database_name, response)
    return response

def api(*args, **kwargs): return OperatorApi.get("table", "list", *args, **kwargs)



