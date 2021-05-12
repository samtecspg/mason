from typing import Optional, Union

from mason.configurations.config import Config
from mason.engines.execution.models.jobs.query_job import QueryJob
from mason.engines.metastore.models.table.table import Table
from mason.engines.storage.models.path import InvalidPath
from mason.util.result import compute

from mason.clients.response import Response
from mason.engines.execution.models.jobs import Job, ExecutedJob, InvalidJob
from mason.engines.metastore.models.database import Database
from mason.engines.metastore.models.ddl import DDLStatement
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import DelayedOperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableInfer(OperatorDefinition):
    
    def run_async(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> DelayedOperatorResponse:
        database_name: str = parameters.get_required("database_name")
        storage_path: str = parameters.get_required("storage_path")
        table_name: Optional[str] = parameters.get_optional("table_name")
        read_headers: bool = isinstance(parameters.get_optional("read_headers"), str)
        path = config.storage().get_path(storage_path)

        final: Union[ExecutedJob, InvalidJob]
        if isinstance(path, InvalidPath):
            final = InvalidJob(f"Invalid storage_path: {path.reason}")
        else:
            table, response = config.storage().infer_table(path, table_name, {"read_headers": read_headers}, response)
            if isinstance(table, Table):
                response.add_info(f"Table inferred: {table.name}")
                database, response = config.metastore().get_database(database_name, response)
                db = compute(database)
                if isinstance(db, Database):
                    ddl = config.metastore().generate_table_ddl(table, path, db)
                    if isinstance(ddl, DDLStatement):
                        job = QueryJob(ddl.statement, db.tables.tables[0])
                        final, response = config.execution().run_job(job, response)
                    else:
                        final = InvalidJob(f"Invalid DDL generated: {ddl.reason}")
                else:
                    final = InvalidJob(f"Metastore database {database_name} not found")
            else:
                response = table.to_response(response)
                final = InvalidJob(f"Invalid Tables: {table.message()}")
            
        return DelayedOperatorResponse(final, response)

