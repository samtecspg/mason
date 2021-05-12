from typing import Union, Optional

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob
from mason.engines.execution.models.jobs.summary_job import SummaryJob
from mason.engines.metastore.models.credentials import MetastoreCredentials, InvalidCredentials
from mason.engines.metastore.models.table.table import Table
from mason.engines.storage.models.path import Path, InvalidPath
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse, DelayedOperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableSummarize(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        table_path: str = parameters.get_required("table_path")
        read_headers: bool = isinstance(parameters.get_optional("read_headers"), str)
        options = {"read_headers": read_headers}
        
        table, response = config.metastore().get_table(table_path, options, response)
        if isinstance(table, Table):
            summary, response = config.metastore().summarize_table(table, options, response)
        else:
            summary = table
            
        return OperatorResponse(response, summary)
    
    def run_async(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> DelayedOperatorResponse:
        table_path: str = parameters.get_required("table_path")
        read_headers: bool = isinstance(parameters.get_optional("read_headers"), str)
        out_path: Optional[str] = parameters.get_optional("output_path")
        if out_path:
            output_path: Union[Path, InvalidPath] = config.storage().get_path(out_path)
            if isinstance(output_path, Path):
                credentials: Union[MetastoreCredentials, InvalidCredentials] = config.metastore().credentials()
                if isinstance(credentials, MetastoreCredentials):
                    input_path = config.storage().get_path(table_path)
                    if isinstance(input_path, Path):
                        job = SummaryJob(input_path, output_path, credentials, read_headers)
                        run, response = config.execution().run_job(job)
                    else:
                        run = InvalidJob(f"Invalid input_path: {input_path.reason}")
                else:
                    run = InvalidJob("Invalid Metastore Credentials")
            else:
                run = InvalidJob(f"Invalid output path: {output_path.reason}")
        else:
            run = InvalidJob("Must specify output_path for asynchronous execution client")
        
        return DelayedOperatorResponse(run, response)
