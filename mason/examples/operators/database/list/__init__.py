from mason.configurations.config import Config

from mason.clients.response import Response
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class DatabaseList(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        databases, response = config.metastore().get_databases(response)
        return OperatorResponse(response, databases)


