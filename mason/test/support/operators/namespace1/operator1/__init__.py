from mason.engines.metastore.models.schemas.schema import EmptySchema

from mason.engines.metastore.models.table import Table

from mason.clients.response import Response
from mason.operators.operator_response import OperatorResponse

from mason.parameters.validated_parameters import ValidatedParameters
from mason.configurations.valid_config import ValidConfig
from mason.util.environment import MasonEnvironment


def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
    table = Table("test_table", EmptySchema())
    response.add_info("Running operator1")
    return OperatorResponse(response, table)
