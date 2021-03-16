from typing import Optional, Union

from mason_dask.jobs.executed import ExecutedJob

from mason.configurations.config import Config
from mason.engines.execution.models.jobs import InvalidJob
from mason.engines.execution.models.jobs.format_job import FormatJob
from mason.engines.metastore.models.credentials import MetastoreCredentials
from mason.engines.metastore.models.table import Table

from mason.clients.response import Response
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableFormat(OperatorDefinition):
    def run_async(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> Union[ExecutedJob, InvalidJob]:
        table_name: str = parameters.get_required("table_name")
        database_name: str = parameters.get_required("database_name")
        output_path: str = parameters.get_required("output_path")
        format: str = parameters.get_required("format")
        sample_size: str = parameters.get_optional("sample_size") or "3"
        
        partition_columns: Optional[str] = parameters.get_optional("partition_columns")
        filter_columns: Optional[str] = parameters.get_optional("filter_columns")
        partitions: Optional[str] = parameters.get_optional("partitions")

        outp = config.storage().path(output_path)
        table, response = config.metastore().get_table(database_name, table_name, options={"sample_size": sample_size}, response=response)
        credentials = config.metastore().credentials()
        
        if isinstance(credentials, MetastoreCredentials):
            if isinstance(table, Table):
                job = FormatJob(table, outp, format, partition_columns, filter_columns, partitions, credentials)
                executed, response = config.execution().run_job(job, response)
            else:
                message = f"Table not found: {table_name}, {database_name}. Messages:  {table.message()}"
                executed = InvalidJob(message)
        else:
            message = f"Invalid Metastore Credentials: {credentials.reason}"
            executed = InvalidJob(message)

        return executed

