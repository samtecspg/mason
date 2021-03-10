from mason.clients.response import Response
from mason.configurations.config import Config
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class JobGet(OperatorDefinition):

    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        job_id: str = parameters.get_required("job_id")
        execution = config.execution()
        job, response = config.execution().get_job(job_id, response)
        return OperatorResponse(response, job)

