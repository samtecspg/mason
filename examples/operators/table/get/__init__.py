from configurations.valid_config import ValidConfig
from engines.metastore.models.table import Table, InvalidTable
from parameters import ValidatedParameters
from clients.response import Response
from api import operator_api as OperatorApi
from util.environment import MasonEnvironment

def api(*args, **kwargs): return OperatorApi.get("table", "get", *args, **kwargs)

def run(env: MasonEnvironment, config: ValidConfig, parameters: ValidatedParameters, response: Response):
    database_name: str = parameters.get_required("database_name")
    table_name: str = parameters.get_required("table_name")

    table = config.metastore.client.get_table(database_name, table_name)
    if isinstance(table, Table):
        response.add_data(table.to_dict())
    else:
        if isinstance(table, InvalidTable):
            tables = [table]
        else:
            tables = table

        for table in tables:
            if table.schema_conflict:
                response.add_data(table.schema_conflict.to_dict())
            response.add_error(table.reason)
            if "not found" in table.reason:
                response.set_status(404)

    return response

