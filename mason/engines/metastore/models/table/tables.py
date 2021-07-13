from typing import Optional, Tuple, Union

from pandas import DataFrame

from engines.metastore.models.table.populated_table import PopulatedTable
from mason.clients.engines.storage import StorageClient
from mason.clients.response import Response
from mason.engines.metastore.models.schemas import schemas
from mason.engines.metastore.models.schemas.schema import Schema, InvalidSchema, EmptySchema
from mason.engines.metastore.models.table.invalid_table import InvalidTables, InvalidTable, TableNotFound
from mason.engines.metastore.models.table.summary import TableSummary, from_table
from mason.engines.metastore.models.table.table import Table
from mason.engines.storage.models.path import Path, InvalidPath
from mason.util.exception import message
from mason.util.list import sequence
from mason.util.uuid import uuid4

def generate_table_name(table_name: Optional[str]):
    if table_name:
        return table_name
    else:
        "mason-generated-table-" + str(uuid4())

def infer(path: Union[Path, InvalidPath], client: StorageClient, name: Optional[str] = None, options: dict = {}, response: Response=Response()) -> Tuple[Union[Table, InvalidTables], Response]:
    ss = options.get("sample_size")
    if not isinstance(ss, int):
        #  TODO:  Move sample size into local client
        # response.add_warning(f"Invalid sample_size: {ss}")
        ss = 3
    final: Union[Table, InvalidTables]
    
    if isinstance(path, Path):
        # TODO: fix this last condition
        paths, response = client.expand_path(path, response, ss)
        response.add_debug(f"{len(paths)} sub paths sampled at {path.full_path()}")

        if len(paths) > 0:
            try:
                valid, invalid_schemas = sequence(list(map(lambda path: schemas.from_path(path, client, options, response), paths)), Schema, InvalidSchema)
                non_empty = [v for v in valid if not isinstance(v, EmptySchema)]
                from mason.engines.metastore.models.schemas import check_schemas as CheckSchemas
                validated, paths = CheckSchemas.find_conflicts(non_empty)
                table = CheckSchemas.get_table(generate_table_name(path.display_table_name() or name), validated, paths, source_path=path)
                invalid_tables = list(map(lambda i: InvalidTable("Invalid Schema", invalid_schema=i), invalid_schemas))
                if isinstance(table, Table):
                    final = table
                else:
                    invalid_tables.append(table)
                    final = InvalidTables(invalid_tables)
            except Exception as e:
                final = InvalidTables([InvalidTable(f"Not able to infer table: {message(e)}")])
        else:
            response.set_status(404)
            final = InvalidTables([TableNotFound(f"No keys at {path.full_path()}")])
    else:
        final = InvalidTables([InvalidTable(f"InvalidPath:{path.reason}")])

    return final, response


def summarize(table: Table, client: StorageClient, options: dict = {}, response: Response = Response()) -> Tuple[Union[TableSummary, InvalidTables], Response]:
    sp = table.source_path
    if sp:
        populated = table.populate(sp, client)
        if isinstance(populated, PopulatedTable):
            return from_table(populated, response)
        else:
            return InvalidTables([populated]), response
    else:
        return InvalidTables([], "Source path not found"), response

def query(table: PopulatedTable, query: str, response: Response) -> Optional[DataFrame]:
    try:
        from dask_sql import Context
        c = Context()
        c.create_table("df", table.ddf())
        return c.sql(query).compute()
    except Exception as e:
        response.add_error(f"Error executing SQL query: {message(e)}")
        return None
