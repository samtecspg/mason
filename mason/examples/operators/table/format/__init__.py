from mason.engines.execution.models.jobs import InvalidJob, ExecutedJob
from mason.engines.execution.models.jobs.format_job import FormatJob
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
        
        outp = config.storage.client.path(output_path)
        table, response = config.metastore.client.get_table(database_name, table_name, response=response)
        if isinstance(table, Table):
            job = FormatJob(table, outp, format)
            executed, response = config.execution.client.run_job(job, response)
            if isinstance(executed, ExecutedJob):
                executed = job.running()
            else:
                executed = InvalidJob(f"Job {job.id} errored: {executed.reason}")
        else:
            message = f"Table not found: {table_name}, {database_name}"
            executed = InvalidJob(message)

        return OperatorResponse(response, executed)

