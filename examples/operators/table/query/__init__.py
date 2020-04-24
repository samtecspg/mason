from configurations import Config
from util.environment import MasonEnvironment
from parameters import Parameters
from clients.response import Response
from api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("table", "merge", *args, **kwargs)

def run(env: MasonEnvironment, config: Config, parameters: Parameters, response: Response):
    query_string = parameters.safe_get("query_string")
    database_name = parameters.safe_get("database_name")
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



