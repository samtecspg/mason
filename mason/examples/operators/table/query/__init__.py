from typing import Union

from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.engines.execution.models.jobs import InvalidJob
from mason.engines.execution.models.jobs.executed_job import ExecutedJob
from mason.engines.execution.models.jobs.query_job import QueryJob
from mason.engines.metastore.models.database import Database
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment
from mason.api import operator_api as OperatorApi

def api(*args, **kwargs): return OperatorApi.get("table", "query", *args, **kwargs)

class TableQuery(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        query_string = parameters.get_required("query_string")
        database_name = parameters.get_required("database_name")
        output_path = parameters.get_optional("output_path")

        # TODO?: Sanitize the query string
        query = query_string
        final: Union[ExecutedJob, InvalidJob]

        database, response = config.metastore.client.get_database(database_name)
        
        if output_path:
            outp = config.storage.client.path(output_path)
        else:
            outp = None
            
        if isinstance(database, Database):
            response.add_info(f"Running Query \"{query}\"")
            job = QueryJob(query_string, database, outp)
            final, response = config.execution.client.run_job(job, response)
        else:
            response.add_error(f"Database not found {database_name}")
            final = InvalidJob("Database not found")

        return OperatorResponse(response, final)



