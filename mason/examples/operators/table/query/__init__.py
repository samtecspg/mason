from typing import Union, Optional

from mason.clients.engines.storage import StorageClient
from mason.clients.response import Response
from mason.configurations.config import Config
from mason.engines.execution.models.jobs import InvalidJob
from mason.engines.execution.models.jobs.executed_job import ExecutedJob
from mason.engines.execution.models.jobs.query_job import QueryJob
from mason.engines.metastore.models.table.table import Table
from mason.engines.storage.models.path import Path
from mason.operators.operator_definition import OperatorDefinition
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableQuery(OperatorDefinition):
    def run_async(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> Union[ExecutedJob, InvalidJob]:
        query_string = parameters.get_required("query_string")
        database_name = parameters.get_required("database_name")
        table_name = parameters.get_required("table_name")
        output_path = parameters.get_optional("output_path")

        # TODO?: Sanitize the query string
        query = query_string
        final: Union[ExecutedJob, InvalidJob]

        table, response = config.metastore().get_table(database_name, table_name)
        
        if output_path and isinstance(config.storage(), StorageClient):
            outp: Optional[Path] = config.storage().path(output_path)
        else:
            outp = None

        if isinstance(table, Table):
            response.add_info(f"Running Query \"{query}\"")
            job = QueryJob(query_string, table, outp)
            final, response = config.execution().run_job(job, response)
        else:
            final = InvalidJob(table.message())

        return final



