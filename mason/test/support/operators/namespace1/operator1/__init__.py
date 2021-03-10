from mason.configurations.config import Config
from mason.engines.metastore.models.schemas.schema import EmptySchema

from mason.engines.metastore.models.table import Table

from mason.clients.response import Response
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse

from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment


class Namespace1Operator1(OperatorDefinition):

    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        table = Table("test_table", EmptySchema())
        response.add_info("Running operator1")
        return OperatorResponse(response, table)
