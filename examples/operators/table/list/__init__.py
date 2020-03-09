from clients.response import Response
from parameters import Parameters
from configurations import Config
import operators as Operator

def run(config: Config, parameters: Parameters, response: Response):
    database_name: str = parameters.safe_get("database_name")
    response = config.metastore.client.list_tables(database_name, response)

    return response

# TODO: Automate this
def api(database_name: str):
    config = Config()
    parameters = f"database_name:{database_name}"
    params = Parameters(parameters)
    response = Operator.run(config, params, "table", "list")

    return response.formatted(), response.status_code
