from typing import Set

from configurations import Config
from util.environment import MasonEnvironment
from parameters import Parameters
from clients.response import Response
from api import operator_api as OperatorApi
from engines.metastore.models.credentials import MetastoreCredentials
from util.logger import logger


def api(*args, **kwargs): return OperatorApi.get("table", "merge", *args, **kwargs)

def run(env: MasonEnvironment, config: Config, parameters: Parameters, response: Response):
    SUPPORTED_SCHEMAS = {
        "parquet"
    }

    response = Response()

    metastore_client = config.metastore.client
    metastore_credentials: MetastoreCredentials = metastore_client.credentials()

    input_path = metastore_client.full_path(parameters.safe_get("input_path"))
    output_path = metastore_client.full_path(parameters.safe_get("output_path"))

    input_database, input_table = metastore_client.parse_path(input_path)

    schemas, response = metastore_client.get_table(input_database, input_table, response)
    schema_types: Set[str] = set(map(lambda schema: schema.type, schemas))
    if len(schemas) > 0 and schema_types.issubset(SUPPORTED_SCHEMAS):
        p = {'input_path': input_path, 'output_path': output_path}
        response = config.execution.client.run_job("merge", metastore_credentials, p, response)
    else:
        response.add_error(f"Unsupported schemas for merge operator: {', '.join(list(schema_types.difference(SUPPORTED_SCHEMAS)))}")

    return response


