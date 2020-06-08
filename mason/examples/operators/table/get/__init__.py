from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.api import operator_api as OperatorApi
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

def api(*args, **kwargs): return OperatorApi.get("table", "get", *args, **kwargs)

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, resp: Response) -> OperatorResponse:
    database_name: str = parameters.get_required("database_name")
    table_name: str = parameters.get_required("table_name")

    table, response = config.metastore.client.get_table(database_name, table_name, response=resp)
    return OperatorResponse(response, table)

