from configurations import Config
from parameters import Parameters
from clients.response import Response
from api import operator_api as OperatorApi
from util.environment import MasonEnvironment

def api(*args, **kwargs): return OperatorApi.get("table", "refresh", *args, **kwargs)

def run(env: MasonEnvironment, config: Config, parameters: Parameters, response: Response):
    database_name: str = parameters.safe_get("database_name")
    table_name: str = parameters.safe_get("table_name")

    # TODO: break this up into 2 calls between scheduler and metastore, remove trigger_schedule_for_table from engine definition, table may not be defined for scheduler
    response = config.scheduler.client.trigger_schedule_for_table(table_name, database_name, response)

    return response
