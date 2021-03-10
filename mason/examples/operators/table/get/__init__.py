from mason.clients.response import Response
from mason.configurations.config import Config
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableGet(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, resp: Response) -> OperatorResponse:
        database_name: str = parameters.get_required("database_name")
        table_name: str = parameters.get_required("table_name")
        read_headers: bool = isinstance(parameters.get_optional("read_headers"), str)

        table, response = config.metastore().get_table(database_name, table_name, {"read_headers": read_headers}, resp)
        oR = OperatorResponse(response, table)
        return oR
