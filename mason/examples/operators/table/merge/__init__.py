from typing import Set, Union

from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.engines.execution.models.jobs import InvalidJob
from mason.engines.execution.models.jobs.executed_job import ExecutedJob
from mason.engines.execution.models.jobs.merge_job import MergeJob
from mason.engines.metastore.models.table import Table
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment
from mason.api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("table", "merge", *args, **kwargs)

class TableMerge(OperatorDefinition):

    def run(self, env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        SUPPORTED_SCHEMAS = {
            "parquet",
            "csv",
            "json",
            "jsonl"
        }

        input_path = parameters.get_required("input_path")
        output_path = parameters.get_required("output_path")
        parse_headers = parameters.get_optional("parse_headers")

        table, response  = config.storage.client.infer_table(input_path, "input_table", {"read_headers": parse_headers}, response)
        final: Union[ExecutedJob, InvalidJob]

        if isinstance(table, Table):
            final = InvalidJob("No conflicting schemas found. Merge Unecessary")
        else:
            conflicting_table = table.conflicting_table()
            if conflicting_table:
                schemas = conflicting_table.schema_conflict.unique_schemas
                schema_types: Set[str] = set(map(lambda schema: schema.type, schemas))
                job = MergeJob(config.storage.client.path(input_path), config.storage.client.path(output_path), next(iter(schema_types)))
                if len(schemas) > 0 and schema_types.issubset(SUPPORTED_SCHEMAS):
                    if len(schema_types) == 1:
                        executed, response = config.execution.client.run_job(job, response)
                        if isinstance(executed, ExecutedJob):
                            final = job.running()
                        else:
                            final = InvalidJob(f"Job {job.id} errored: {executed.reason}")
                    else:
                        final = InvalidJob("Mixed schemas not supported at this time.")
                else:
                    final = InvalidJob(f"Unsupported schemas for merge operator: {', '.join(list(schema_types.difference(SUPPORTED_SCHEMAS)))}")
            else:
                final = InvalidJob(f"No conflicting schemas found at {input_path}. Merge unecessary. Invalid Schemas {table.message()}")
                
        return OperatorResponse(response, final) 
