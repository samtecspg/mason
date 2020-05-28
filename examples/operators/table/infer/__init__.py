from typing import Optional, List

from configurations.valid_config import ValidConfig
from engines.execution.models.jobs import ExecutedJob
from engines.metastore.models.database import Database
from engines.metastore.models.ddl import DDLStatement
from engines.metastore.models.table import Table, InvalidTable
from parameters import ValidatedParameters
from clients.response import Response
from api import operator_api as OperatorApi
from util.environment import MasonEnvironment

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):

    database_name: str = parameters.get_required("database_name")
    storage_path: str = parameters.get_required("storage_path")
    output_path: Optional[str] = parameters.get_optional("output_path")

    table = config.storage.client.infer_table(storage_path)

    if isinstance(table, Table):
        database = config.metastore.client.get_database(database_name)
        if isinstance(database, Database):
            op = config.storage.client.path(output_path)
            ddl = config.metastore.client.generate_table_ddl(table, op)
            if isinstance(ddl, DDLStatement):
                executed = config.metastore.client.execute_ddl(ddl, database)
                if isinstance(executed, ExecutedJob):
                    response = executed.job.running().job.response
                else:
                    response.add_error(f"Job errored: {executed.reason}")
            else:
                response.add_error(f"Invalid DDL generated: {ddl.reason}")
        else:
            response.add_error(f"Metastore database {database_name} not found")
    else:
        invalid: List[InvalidTable]
        if isinstance(table, InvalidTable):
           invalid = [table]
        else:
            invalid = table
        messages = ", ".join(list(map(lambda i: i.reason, invalid)))
        response.add_warning(f"Invalid Tables: {messages}")
        response.add_error(f"No valid tables could be inferred at {storage_path}")

    return response

def api(*args, **kwargs): return OperatorApi.get("table", "infer", *args, **kwargs)
