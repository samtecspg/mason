from configurations import Config
from parameters import Parameters
from clients.response import Response
import operators as Operator

def run(config: Config, parameters: Parameters, response: Response):
    database_name: str = parameters.safe_get("database_name")
    table_name: str = parameters.safe_get("table_name")

    response = config.metastore.client.get_table(database_name, table_name, response)

    return response

# TODO: Automate this
def api(table_name: str, database_name: str):
    config = Config()
    parameters = f"database_name:{database_name},table_name:{table_name}"
    params = Parameters(parameters)
    response = Operator.run(config, params, "table", "get")

    return response.formatted(), response.status_code

