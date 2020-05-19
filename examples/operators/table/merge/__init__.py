from typing import Set

from configurations.valid_config import ValidConfig
from engines.execution.models.jobs.merge_job import MergeJob
from util.environment import MasonEnvironment
from parameters import ValidatedParameters
from clients.response import Response
from api import operator_api as OperatorApi
from engines.metastore.models.credentials import MetastoreCredentials

def api(*args, **kwargs): return OperatorApi.get("table", "merge", *args, **kwargs)

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):
    SUPPORTED_SCHEMAS = {
        "parquet",
        "text",
        "json",
        "jsonl"
    }

    response = Response()

    metastore_client = config.metastore.client
    metastore_credentials, response = metastore_client.credentials()
    if not response.errored():

        input_path = parameters.get_required("input_path")
        output_path =parameters.get_required("output_path")
        parse_headers = bool(parameters.get_optional("parse_headers"))
        input_database, input_table = metastore_client.parse_path(input_path)

        schemas, response = metastore_client.get_table(input_database, input_table, response, {"read_headers": parse_headers})
        schema_types: Set[str] = set(map(lambda schema: schema.type, schemas)) # TODO: Ensure this has length one, collapse list to singleton earlier

        if len(schemas) > 0 and schema_types.issubset(SUPPORTED_SCHEMAS):
            job = MergeJob(metastore_client.full_path(input_path), metastore_client.full_path(output_path), next(iter(schema_types)))
            response = config.execution.client.run_job(job, response)
        else:
            response.add_error(f"Unsupported schemas for merge operator: {', '.join(list(schema_types.difference(SUPPORTED_SCHEMAS)))}")

    return response


