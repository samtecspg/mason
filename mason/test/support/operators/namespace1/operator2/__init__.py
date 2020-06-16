from mason.clients.response import Response
from mason.operators.operator_response import OperatorResponse

from mason.parameters.validated_parameters import ValidatedParameters
from mason.configurations.valid_config import ValidConfig
from mason.util.environment import MasonEnvironment


def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
    response.add_info("Running operator2")
    return OperatorResponse(response)
