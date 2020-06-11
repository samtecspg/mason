from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.api import operator_api as OperatorApi
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

def api(*args, **kwargs): return OperatorApi.get("table", "refresh", *args, **kwargs)

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
    database_name: str = parameters.get_required("database_name")
    table_name: str = parameters.get_required("table_name")

    # TODO: break this up into 2 calls between scheduler and metastore, remove trigger_schedule_for_table from engine definition, table may not be defined for scheduler
    response = config.scheduler.client.trigger_schedule_for_table(table_name, database_name, response)
    return OperatorResponse(response)

