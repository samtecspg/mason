from typing import Set, List

from configurations.valid_config import ValidConfig
from engines.execution.models.jobs import ExecutedJob
from engines.execution.models.jobs.merge_job import MergeJob
from engines.metastore.models.table import InvalidTable, Table
from util.environment import MasonEnvironment
from parameters import ValidatedParameters
from clients.response import Response
from api import operator_api as OperatorApi

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

        input_path = config.storage.client.get_path(parameters.get_required("input_path"))
        output_path =config.storage.client.get_path(parameters.get_required("output_path"))
        parse_headers = parameters.get_optional("parse_headers")

        table = config.storage.client.infer_table("input_table", input_path.path_str, {"read_headers": parse_headers})

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
                        job = MergeJob(input_path, output_path, next(iter(schema_types)))
                        executed = config.execution.client.run_job(job, response)
                        if isinstance(executed, ExecutedJob):
                            response = executed.job.running().response
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


