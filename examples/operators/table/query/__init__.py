from configurations.valid_config import ValidConfig
from util.environment import MasonEnvironment
from parameters import ValidatedParameters
from clients.response import Response
from api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("table", "query", *args, **kwargs)

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):
    query_string = parameters.get_required("query_string")
    database_name = parameters.get_required("database_name")
    metastore_client = config.metastore.client

    # TODO?: Sanitize the query string
    # query = metastore_client.sanitize_query()
    query = query_string

    metastore_credentials = metastore_client.credentials()

    if metastore_credentials.empty():
        response.add_error("Metastore credentials empty")
    else:
        response.add_info(f"Running Query \"{query}\"")
        p = { 'query_string': query_string, 'database_name': database_name}
        response = config.execution.client.run_job("query", metastore_credentials, p, response)

    return response



