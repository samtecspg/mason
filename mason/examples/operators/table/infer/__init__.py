from typing import Optional, Union

from mason.configurations.config import Config
from mason.util.result import compute

from mason.clients.response import Response
from mason.engines.execution.models.jobs import Job, ExecutedJob, InvalidJob
from mason.engines.metastore.models.database import Database
from mason.engines.metastore.models.ddl import DDLStatement
from mason.engines.metastore.models.table import Table
from mason.operators.operator_definition import OperatorDefinition
from mason.operators.operator_response import OperatorResponse
from mason.parameters.validated_parameters import ValidatedParameters
from mason.util.environment import MasonEnvironment

class TableInfer(OperatorDefinition):
    def run(self, env: MasonEnvironment, config: Config, parameters: ValidatedParameters, response: Response) -> OperatorResponse:
        database_name: str = parameters.get_required("database_name")
        storage_path: str = parameters.get_required("storage_path")
        table_name: Optional[str] = parameters.get_optional("table_name")

        path = config.storage().path(storage_path)
        table, response = config.storage().infer_table(path, table_name, response=response)
        job = Job("query")
        final: Union[ExecutedJob, InvalidJob]

        if isinstance(table, Table):
            response.add_info(f"Table inferred: {table.name}")
            database, response = config.metastore().get_database(database_name, response)
            db = compute(database)
            if isinstance(db, Database):
                ddl = config.metastore().generate_table_ddl(table, path, db)
                if isinstance(ddl, DDLStatement):
                    final, response = config.metastore().execute_ddl(ddl, db, response)
                else:
                    final = job.errored(f"Invalid DDL generated: {ddl.reason}")
            else:
                final = job.errored(f"Metastore database {database_name} not found")
        else:
            response = table.to_response(response)
            final = job.errored(f"Invalid Tables: {table.message()}")
            
        return OperatorResponse(response, final)

