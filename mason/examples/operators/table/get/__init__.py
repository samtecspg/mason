from mason.clients.response import Response
from mason.configurations.config import Config
from mason.engines.execution.models.jobs import InvalidJob
from mason.engines.execution.models.jobs.infer_job import InferJob
from mason.engines.metastore.models.credentials import MetastoreCredentials
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse, DelayedOperatorResponse
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
    
    def run_async(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, resp: Response) -> DelayedOperatorResponse:
        database_name: str = parameters.get_required("database_name")
        table_name: str = parameters.get_required("table_name")
        read_headers: bool = isinstance(parameters.get_optional("read_headers"), str)
        
        path = config.storage().table_path(database_name, table_name)
        credentials = config.metastore().credentials()

        if isinstance(credentials, MetastoreCredentials):
            job = InferJob(database_name, table_name, path, credentials, read_headers)
            run, response = config.execution().run_job(job, resp)
        else:
            run = InvalidJob(f"Invalid MetastoreCredentials: {credentials.reason}")
            
        return DelayedOperatorResponse(run, resp or Response())


