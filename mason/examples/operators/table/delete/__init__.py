from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment
from mason.api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("table", "merge", *args, **kwargs)

class TableDelete(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        database_name: str = parameters.get_required("database_name")
        table_name: str = parameters.get_required("table_name")

        response = config.metastore.client.delete_table(database_name, table_name, response)

        return OperatorResponse(response)



