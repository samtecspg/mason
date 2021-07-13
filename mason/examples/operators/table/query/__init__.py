from typing import Union, Optional, Tuple

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.engines.execution.models.jobs import InvalidJob, Job
from mason.engines.execution.models.jobs.executed_job import ExecutedJob
from mason.engines.execution.models.jobs.query_job import QueryJob
from mason.engines.metastore.models.table.invalid_table import InvalidTables
from mason.engines.metastore.models.table.table import Table
from mason.engines.storage.models.path import Path, InvalidPath
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import DelayedOperatorResponse, OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableQuery(OperatorDefinition):
    
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        table_path: str = parameters.get_required("table_path")
        query_string: str = parameters.get_required("query_string")
        output_path: Optional[str] = parameters.get_optional("output_path")

        # TODO?: Sanitize the query string
        query = query_string
        final: Union[ExecutedJob, InvalidJob] = InvalidJob("Bad")
        table, response = config.metastore().get_table(table_path)
        
        if output_path:
            outp: Union[Path, InvalidPath] = config.storage().get_path(output_path)
        else:
            outp = Path("stdout")

        if isinstance(table, Table) and not isinstance(outp, InvalidPath):
            response.add_info(f"Running Query \"{query}\"")
            job = QueryJob(query_string, table, outp)
            final, response = config.execution().run_job(job, response)
        else:
            if isinstance(table, InvalidTables):
                final = InvalidJob(table.message())
            elif isinstance(outp, InvalidPath):
                final = InvalidJob(f"Invalid output Path: {outp.reason}")
                
        return OperatorResponse(response, final)


    def run_async(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> DelayedOperatorResponse:
        table_path: str = parameters.get_required("table_path")
        query_string: str = parameters.get_required("query_string")
        output_path: Optional[str] = parameters.get_optional("output_path")

        # TODO?: Sanitize the query string
        query = query_string
        final: Union[ExecutedJob, InvalidJob]
        table, response = config.metastore().get_table(table_path)
        if output_path:
            outp: Union[Path, InvalidPath] = config.storage().get_path(output_path)

        if isinstance(table, Table) and not isinstance(outp, InvalidPath):
            response.add_info(f"Running Query \"{query}\"")
            job = QueryJob(query_string, table, outp)
            final, response = config.execution().run_job(job, response)
        else:
            if isinstance(table, InvalidTables):
                final = InvalidJob(table.message())
            elif isinstance(outp, InvalidPath):
                final = InvalidJob(f"Invalid output Path: {outp.reason}")

        return DelayedOperatorResponse(final, response)