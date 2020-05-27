from configurations.valid_config import ValidConfig
from engines.execution.models.jobs import ExecutedJob
from engines.execution.models.jobs.query_job import QueryJob
from engines.metastore.models.database import Database
from util.environment import MasonEnvironment
from parameters import ValidatedParameters
from clients.response import Response
from api import operator_api as OperatorApi

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



