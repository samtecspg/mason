from typing import Optional, Union

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.engines.execution.models.jobs import InvalidJob, ExecutedJob
from mason.engines.execution.models.jobs.preview_job import PreviewJob
from mason.engines.metastore.models.credentials import MetastoreCredentials
from mason.engines.metastore.models.table.table import Table
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse, DelayedOperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableGet(OperatorDefinition):
    
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        database_name: str = parameters.get_required("database_name")
        table_name: str = parameters.get_required("table_name")
        read_headers: bool = isinstance(parameters.get_optional("read_headers"), str)

        table, response = config.metastore().get_table(database_name, table_name, {"read_headers": read_headers}, response)
        return OperatorResponse(response, table)
    
    
    def run_async(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> DelayedOperatorResponse:
        database_name: str = parameters.get_required("database_name")
        table_name: str = parameters.get_required("table_name")
        read_headers: bool = isinstance(parameters.get_optional("read_headers"), str)
        output_path: Optional[str] = parameters.get_optional("output_path") 
        final: Union[InvalidJob, ExecutedJob]
        ip = config.storage().table_path(database_name, table_name)

        table, response = config.metastore().get_table(database_name, table_name, {"read_headers": read_headers}, response)
        
        if isinstance(table, Table):
            if output_path:
                credentials = config.metastore().credentials()
                if isinstance(credentials, MetastoreCredentials):
                    op = config.storage().path(output_path)
                    job = PreviewJob(ip, table.schema.type, op, credentials, read_headers)
                    final, response = config.execution().run_job(job, response)
                else:
                    final = InvalidJob("Metastore credentials required for asynchronous execution engine.")
            else:
                final = InvalidJob("Output path required for asynchronous execution engine.")
        else:
            final = InvalidJob(f"Invalid table: {table.message()}")

        return DelayedOperatorResponse(final, response)
    