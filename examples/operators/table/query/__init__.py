from configurations.valid_config import ValidConfig
from engines.execution.models.jobs.query_job import QueryJob
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

    metastore_credentials, response = metastore_client.credentials(response)

    if not response.errored():
        response.add_info(f"Running Query \"{query}\"")
        job = QueryJob(database_name, query_string, metastore_credentials.to_dict())
        response = config.execution.client.run_job(job, response)

    return response



