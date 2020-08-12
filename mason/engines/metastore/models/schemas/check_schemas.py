from typing import List, Union, Tuple
from functools import reduce

from mason.engines.metastore.models.schemas.schema import Schema, InvalidSchema
from mason.engines.metastore.models.schemas.json import merge_json_schemas, JsonSchema
from mason.engines.metastore.models.schemas.schema import SchemaConflict
from mason.engines.metastore.models.table import Table, InvalidTable, ConflictingTable
from mason.engines.storage.models.path import Path
from mason.util.list import get

def diff_schemas(schema1: Schema, schema2: Schema) -> Schema:
    column_diff = schema1.diff(schema2)
    #  NOTE: if two schemas differ in type all columns will differ by default
    #  Therefore choosing to take first schema's types arbitrarily
    # TODO: Look into why type isn't working here
    gen: Schema = Schema(list(column_diff), schema1.type, schema1.path)
    return gen

def find_conflicts(schemas: List[Schema]) -> Tuple[Union[Schema, SchemaConflict, InvalidSchema], List[Path]]:

    schema_types = set(map(lambda s: s.type, schemas))

    paths: List[Path]
    if schema_types == {'json'} or schema_types == {'jsonl'} or schema_types == { 'json', 'jsonl' }:
        for schema in schemas:
            assert(isinstance(schema, JsonSchema))
        paths = list(map(lambda s: s.path, schemas))
        return merge_json_schemas(schemas), paths
    elif len(schema_types) > 1:
        return InvalidSchema(f"Mixed type schemas not supported at this time.  Ensure that files are of one type: {schema_types}"), []
    else:
        non_empty_schemas = list(filter(lambda s: s.columns != [], schemas))
        paths = list(map(lambda s: s.path, non_empty_schemas))
        unique_schemas = set(non_empty_schemas)

        if len(unique_schemas) > 1:
            diff = reduce(diff_schemas, unique_schemas)
            return SchemaConflict(list(unique_schemas), diff), paths
        else:
            s = get(list(unique_schemas), 0)
            if s:
                return s, paths
            else:
                return InvalidSchema("No valid schemas found"), paths


def get_table(name: str, schema: Union[Schema, SchemaConflict, InvalidSchema], paths: List[Path]) -> Union[Table, InvalidTable]:
    if isinstance(schema, Schema):
        return Table(name, schema, created_by="mason", paths=paths)
    else:
        if isinstance(schema, InvalidSchema):
            return InvalidTable(f"Invalid Schema: {schema.reason}")
        else:
            return ConflictingTable(schema, "Conflicting Schemas")





