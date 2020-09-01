from typing import Optional

from mason.engines.execution.models.jobs import InvalidJob
from mason.engines.execution.models.jobs.format_job import FormatJob
from mason.engines.metastore.models.credentials import MetastoreCredentials
from mason.engines.metastore.models.table import Table

from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment
from mason.api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("table", "format", *args, **kwargs)

class TableFormat(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        table_name: str = parameters.get_required("table_name")
        database_name: str = parameters.get_required("database_name")
        output_path: str = parameters.get_required("output_path")
        format: str = parameters.get_required("format")
        sample_size: str = parameters.get_optional("sample_size") or "3"
        
        partition_columns: Optional[str] = parameters.get_optional("partition_columns")
        filter_columns: Optional[str] = parameters.get_optional("filter_columns")
        partitions: Optional[str] = parameters.get_optional("partitions")

        outp = config.storage.client.path(output_path)
        table, response = config.metastore.client.get_table(database_name, table_name, options={"sample_size": sample_size}, response=response)
        credentials = config.metastore.client.credentials()
        
        if isinstance(credentials, MetastoreCredentials):
            if isinstance(table, Table):
                job = FormatJob(table, outp, format, partition_columns, filter_columns, partitions, credentials)
                executed, response = config.execution.client.run_job(job, response)
            else:
                message = f"Table not found: {table_name}, {database_name}. Messages:  {table.message()}"
                executed = InvalidJob(message)
        else:
            message = f"Invalid Metastore Credentials: {credentials.reason}"
            executed = InvalidJob(message)

        return OperatorResponse(response, executed)

