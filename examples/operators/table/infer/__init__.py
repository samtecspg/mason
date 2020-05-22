from configurations.valid_config import ValidConfig
from engines.metastore.models.database import Database
from engines.metastore.models.ddl import DDLStatement
from parameters import ValidatedParameters
from clients.response import Response
from api import operator_api as OperatorApi
from util.environment import MasonEnvironment

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):

    database_name: str = parameters.get_required("database_name")
    storage_path: str = parameters.get_required("storage_path")
    output_path: str = parameters.get_optional("output_path")

    valid, invalid = config.storage.client.infer_table(storage_path)

    if len(invalid) > 0:
        messages = ", ".join(list(map(lambda i: i.reason, invalid)))
        response.add_warning(f"Invalid Table: {messages}")

    if valid:
        database = config.metastore.client.get_database(database_name)
        if isinstance(database, Database):
            op = config.storage.client.get_path(output_path)
            ddl = config.metastore.client.generate_table_ddl(database_name, valid, op)
            if isinstance(ddl, DDLStatement):
                job = config.metastore.client.execute_ddl(ddl, database)
            else:
                response.add_error(f"Invalid DDL generated: {ddl.reason}")
        else:
            response.add_error(f"Metastore database {database_name} not found")
    else:
        response.add_error(f"No valid tables could be inferred at {storage_path}")



    return response

def api(*args, **kwargs): return OperatorApi.get("table", "infer", *args, **kwargs)
