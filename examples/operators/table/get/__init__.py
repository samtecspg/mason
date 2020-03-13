from configurations import Config
from parameters import Parameters
from clients.response import Response
from api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("table", "get", *args, **kwargs)

def run(config: Config, parameters: Parameters, response: Response):
    database_name: str = parameters.safe_get("database_name")
    table_name: str = parameters.safe_get("table_name")

    response = config.metastore.client.get_table(database_name, table_name, response)

    return response

