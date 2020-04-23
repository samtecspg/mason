from typing import Set

from configurations import Config
from util.environment import MasonEnvironment
from parameters import Parameters
from clients.response import Response
from api import operator_api as OperatorApi
from engines.metastore.models.credentials import MetastoreCredentials

def api(*args, **kwargs): return OperatorApi.get("table", "merge", *args, **kwargs)

def run(env: MasonEnvironment, config: Config, parameters: Parameters, response: Response):
    SUPPORTED_SCHEMAS = {
        "parquet",
        "text",
        "json",
        "jsonl"
    }

    response = Response()

    metastore_client = config.metastore.client
    metastore_credentials: MetastoreCredentials = metastore_client.credentials()

    if not metastore_credentials.secret_key or not metastore_credentials.access_key:
        response.add_error("Metastore credentials undefined")
    else:
        input_path = parameters.safe_get("input_path")
        output_path =parameters.safe_get("output_path")
        parse_headers = bool(parameters.unsafe_get("parse_headers"))

        input_database, input_table = metastore_client.parse_path(input_path)

        schemas, response = metastore_client.get_table(input_database, input_table, response, {"read_headers": parse_headers})
        schema_types: Set[str] = set(map(lambda schema: schema.type, schemas)) # TODO: Ensure this has length one, collapse list to singleton earlier

        if len(schemas) > 0 and schema_types.issubset(SUPPORTED_SCHEMAS):
            p = {'input_path': metastore_client.full_path(input_path), 'output_path': metastore_client.full_path(output_path), 'input_format': next(iter(schema_types))}
            response = config.execution.client.run_job("merge", metastore_credentials, p, response)
        else:
            response.add_error(f"Unsupported schemas for merge operator: {', '.join(list(schema_types.difference(SUPPORTED_SCHEMAS)))}")

    return response


