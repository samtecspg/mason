from typing import Optional, Union

from mason.util.result import compute

from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.api import operator_api as OperatorApi
from mason.engines.execution.models.jobs import Job, ExecutedJob, InvalidJob
from mason.engines.metastore.models.database import Database
from mason.engines.metastore.models.ddl import DDLStatement
from mason.engines.metastore.models.table import Table
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

def api(*args, **kwargs): return OperatorApi.get("table", "infer", *args, **kwargs)

class TableInfer(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        database_name: str = parameters.get_required("database_name")
        storage_path: str = parameters.get_required("storage_path")
        table_name: Optional[str] = parameters.get_optional("table_name")

        table, response = config.storage.client.infer_table(storage_path, table_name, response=response)
        job = Job("query")
        final: Union[ExecutedJob, InvalidJob]

        if isinstance(table, Table):
            response.add_info(f"Table inferred: {table.name}")
            database, response = config.metastore.client.get_database(database_name, response)
            db = compute(database)
            if isinstance(db, Database):
                path = config.storage.client.path(storage_path)
                ddl = config.metastore.client.generate_table_ddl(table, path, db)
                if isinstance(ddl, DDLStatement):
                    final, response = config.metastore.client.execute_ddl(ddl, db, response)
                else:
                    final = job.errored(f"Invalid DDL generated: {ddl.reason}")
            else:
                final = job.errored(f"Metastore database {database_name} not found")
        else:
            response = table.to_response(response)
            final = job.errored(f"Invalid Tables: {table.message()}")
            
        return OperatorResponse(response, final)

