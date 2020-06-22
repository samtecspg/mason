from mason.clients.response import Response
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse

from mason.parameters.validated_parameters import ValidatedParameters
from mason.configurations.valid_config import ValidConfig
from mason.util.environment import MasonEnvironment

class Namespace2Operator4(OperatorDefinition):

    def run(self, env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        response.add_info("Running operator4")
        return OperatorResponse(response)
