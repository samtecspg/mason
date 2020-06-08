from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment
from mason.api import operator_api as OperatorApi

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
    database_name: str = parameters.get_required("database_name")
    response = config.metastore.client.list_tables(database_name, response)
    return OperatorResponse(response)

def api(*args, **kwargs): return OperatorApi.get("table", "list", *args, **kwargs)



