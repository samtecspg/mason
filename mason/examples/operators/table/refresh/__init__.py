from mason.clients.response import Response
from mason.configurations.config import Config
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableRefresh(OperatorDefinition):
    
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        table_path: str = parameters.get_required("table_path")

        # TODO: break this up into 2 calls between scheduler and metastore, remove trigger_schedule_for_table from engine definition, table may not be defined for scheduler
        response = config.scheduler().trigger_schedule_for_table(table_path, response)
        return OperatorResponse(response)

