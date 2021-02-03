from mason.clients.response import Response
from mason.configurations.config import Config
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse

from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment


class Namespace1Operator2(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        response.add_info("Running operator2")
        return OperatorResponse(response)
