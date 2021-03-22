from typing import Union, Optional

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob
from mason.engines.execution.models.jobs.summary_job import SummaryJob
from mason.engines.metastore.models.credentials import MetastoreCredentials, InvalidCredentials
from mason.engines.metastore.models.table.table import Table
from mason.engines.storage.models.path import Path
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse, DelayedOperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableSummarize(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        database_name: str = parameters.get_required("database_name")
        table_name: str = parameters.get_required("table_name")
        read_headers: bool = isinstance(parameters.get_optional("read_headers"), str)
        options = {"read_headers": read_headers}
        
        table, response = config.metastore().get_table(database_name, table_name, options, response)
        if isinstance(table, Table):
            summary, response = config.metastore().summarize_table(table, options, response)
        else:
            summary = table
            
        return OperatorResponse(response, summary)
    
    def run_async(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> DelayedOperatorResponse:
        database_name: str = parameters.get_required("database_name")
        table_name: str = parameters.get_required("table_name")
        read_headers: bool = isinstance(parameters.get_optional("read_headers"), str)
        out_path: Optional[str] = parameters.get_optional("output_path")

        input_path: Path = config.storage().table_path(database_name, table_name)
        if out_path:
            output_path: Path = config.storage().path(out_path)
            credentials: Union[MetastoreCredentials, InvalidCredentials] = config.metastore().credentials()
            
            if isinstance(credentials, MetastoreCredentials):
                job = SummaryJob(input_path, output_path, credentials, read_headers)
                run, response = config.execution().run_job(job)
            else:
                run = InvalidJob("Invalid Metastore Credentials")
        else:
            run = InvalidJob("Must specify output_path for asynchronous execution client")
        
        return DelayedOperatorResponse(run, response)
