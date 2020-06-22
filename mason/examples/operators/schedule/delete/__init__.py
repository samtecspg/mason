from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment
from mason.api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("schedule", "delete", *args, **kwargs)


class ScheduleDelete(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        schedule_name: str = parameters.get_required("schedule_name")

        response = config.scheduler.client.delete_schedule(schedule_name, response)
        
        return OperatorResponse(response)



