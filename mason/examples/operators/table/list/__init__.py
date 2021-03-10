from mason.configurations.config import Config
from mason.util.result import compute

from mason.clients.response import Response
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableList(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        database_name: str = parameters.get_required("database_name")
        tables, response = config.metastore().list_tables(database_name, response)
        tb = compute(tables)
        return OperatorResponse(response, tb)


