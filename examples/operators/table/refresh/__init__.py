from configurations import Config
from parameters import Parameters
from clients.response import Response
import operators.operators as Operator
from util.environment import MasonEnvironment

def run(config: Config, parameters: Parameters, response: Response):
    database_name: str = parameters.safe_get("database_name")
    table_name: str = parameters.safe_get("table_name")

    # TODO: break this up into 2 calls, remove trigger_schedule_for_table from engine definition
    response = config.scheduler.client.trigger_schedule_for_table(table_name, database_name, response)

    return response

def api(table_name: str, database_name: str):
    env = MasonEnvironment()
    config = Config(env)
    parameters = f"table_name:{table_name},database_name:{database_name}"
    params = Parameters(parameters)
    response = Operator.run(config, params, "table", "refresh")

    return response.formatted(), response.status_code
