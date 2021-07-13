from typing import Optional

from mason.configurations.config import Config
from mason.engines.execution.models.jobs import InvalidJob
from mason.engines.execution.models.jobs.format_job import FormatJob
from mason.engines.metastore.models.credentials import MetastoreCredentials

from mason.clients.response import Response
from mason.engines.metastore.models.table.table import Table
from mason.engines.storage.models.path import Path
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import DelayedOperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableFormat(OperatorDefinition):
    
    def run_async(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> DelayedOperatorResponse:
        table_path: str = parameters.get_required("table_path")
        output_path: str = parameters.get_required("output_path")
        format: str = parameters.get_required("format")
        sample_size: str = parameters.get_optional("sample_size") or "3"
        
        partition_columns: Optional[str] = parameters.get_optional("partition_columns")
        filter_columns: Optional[str] = parameters.get_optional("filter_columns")
        partitions: Optional[str] = parameters.get_optional("partitions")

        outp = config.storage().get_path(output_path)
        table, response = config.metastore().get_table(table_path, options={"sample_size": sample_size}, response=response)
        credentials = config.metastore().credentials()
        
        if isinstance(credentials, MetastoreCredentials):
            if isinstance(table, Table):
                if isinstance(outp, Path):
                    job = FormatJob(table, outp, format, partition_columns, filter_columns, partitions, credentials)
                    executed, response = config.execution().run_job(job, response)
                else:
                    executed = InvalidJob(f"Invalid output path: {outp.reason}")
            else:
                executed = InvalidJob(table.message())
        else:
            message = f"Invalid Metastore Credentials: {credentials.reason}"
            executed = InvalidJob(message)

        return DelayedOperatorResponse(executed, response)

