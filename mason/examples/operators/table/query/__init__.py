from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.engines.execution.models.jobs import ExecutedJob
from mason.engines.execution.models.jobs.query_job import QueryJob
from mason.engines.metastore.models.database import Database
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment
from mason.api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("table", "query", *args, **kwargs)

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):
    query_string = parameters.get_required("query_string")
    database_name = parameters.get_required("database_name")

    # TODO?: Sanitize the query string
    query = query_string

    database = config.metastore.client.get_database(database_name)
    if isinstance(database, Database):
        response.add_info(f"Running Query \"{query}\"")
        job = QueryJob(query_string, database)
        executed = config.execution.client.run_job(job)
        if isinstance(executed, ExecutedJob):
            response = executed.job.running().job.response
        else:
            response.add_error(f"Job errored: {executed.reason}")
            if "access denied" in executed.reason.lower():
                response.set_status(403)
    else:
        response.add_error(f"Database not found {database_name}")

    return response



