from configurations import Config
from util.environment import MasonEnvironment
from parameters import Parameters
from clients.response import Response
from api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("table", "merge", *args, **kwargs)

def run(env: MasonEnvironment, config: Config, parameters: Parameters, response: Response):
    query_string = parameters.safe_get("query_string")


    metastore_client = config.metastore.client

    # TODO: Sanitize the query string
    # query = metastore_client.sanitize_query()
    query = query_string

    metastore_credentials = metastore_client.credentials()

    if not metastore_credentials.secret_key or not metastore_credentials.access_key:
        response.add_error("Metastore credentials undefined")

    else:
        response.add_info(f"Running Query {query}")
        p = { 'query_string': query_string }
        response = config.execution.client.run_job("query", metastore_credentials, p, response)

    response = Response()

    return response



