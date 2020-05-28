from typing import Set

from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.engines.execution.models.jobs import ExecutedJob
from mason.engines.execution.models.jobs.merge_job import MergeJob
from mason.engines.metastore.models.table import Table, InvalidTable
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment
from mason.api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("table", "merge", *args, **kwargs)

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):
    SUPPORTED_SCHEMAS = {
        "parquet",
        "text",
        "json",
        "jsonl"
    }

    response = Response()

    if not response.errored():

        input_path = parameters.get_required("input_path")
        output_path = parameters.get_required("output_path")
        parse_headers = parameters.get_optional("parse_headers")

        table = config.storage.client.infer_table(input_path, "input_table", {"read_headers": parse_headers})

        if isinstance(table, Table):
            response.add_error("No conflicting schemas found. Merge unecessary")
        else:
            if isinstance(table, InvalidTable):
                tables = [table]
            else:
                tables = table

            if len(tables) == 1:
                schemas = table.schema_conflict.unique_schemas
                schema_types: Set[str] = set(map(lambda schema: schema.type, schemas))
                if len(schemas) > 0 and schema_types.issubset(SUPPORTED_SCHEMAS):
                    if len(schema_types) == 1:
                        job = MergeJob(config.storage.client.path(input_path), config.storage.client.path(output_path), next(iter(schema_types)))
                        executed = config.execution.client.run_job(job, response)
                        if isinstance(executed, ExecutedJob):
                            response = executed.job.running().job.response
                        else:
                            response.add_error(f"Job errored: {executed.reason}")
                    else:
                        response.add_error("Mixed schemas not supported at this time.")
                else:
                    response.add_error(f"Unsupported schemas for merge operator: {', '.join(list(schema_types.difference(SUPPORTED_SCHEMAS)))}")
            else:
                messages = ", ".join(list(map(lambda t: t.reason ,tables)))
                response.add_warning(f"Invalid Schemas: {messages}")
                response.add_error("Multiple Invalid Schemas detected.")

    return response


