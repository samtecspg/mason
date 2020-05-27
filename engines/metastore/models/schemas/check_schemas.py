from datetime import datetime
from typing import List, Union

from engines.metastore.models.schemas.json import merge_json_schemas, JsonSchema
from engines.metastore.models.schemas.schema import Schema, InvalidSchema, SchemaConflict, emptySchema
from functools import reduce

from engines.metastore.models.table import Table, InvalidTable
from util.list import get

def diff_schemas(schema1: Schema, schema2: Schema) -> Schema:
    column_diff = schema1.diff(schema2)
    #  NOTE: if two schemas differ in type all columns will differ by default
    #  Therefore choosing to take first schema's types arbitrarily
    # TODO: Look into why type isn't working here
    gen: Schema = Schema(list(column_diff), schema1.type)
    return gen


def find_conflicts(schemas: List[Schema]) -> Union[Schema, SchemaConflict, InvalidSchema]:

    schema_types = set(map(lambda s: s.type, schemas))
    # TODO:  ValidSchema {'Schema': schema.schema}

    if schema_types == {'json'} or schema_types == {'jsonl'} or schema_types == { 'json', 'jsonl' }:
        for schema in schemas:
            assert(isinstance(schema, JsonSchema))
        return merge_json_schemas(schemas)
    elif len(schema_types) > 1:
        return InvalidSchema("Mixed type schemas not supported at this time.  Ensure that files are of one type")
    else:
        non_empty_schemas = filter(lambda s: s.columns != [], schemas)
        unique_schemas = set(non_empty_schemas)

        if len(unique_schemas) > 1:
            diff = reduce(diff_schemas, unique_schemas)
            return SchemaConflict(list(unique_schemas), diff)
        else:
            s = get(list(unique_schemas), 0)
            if s:
                return s
            else:
                return InvalidSchema("No valid schemas found")


def get_table(name: str, schema: Union[Schema, SchemaConflict, InvalidSchema]) -> Union[Table, InvalidTable]:
    if isinstance(schema, Schema):
        return Table(name, schema, created_by="mason")
    else:
        if isinstance(schema, InvalidSchema):
            return InvalidTable(f"Invalid Schema: {schema.reason}")
        else:
            return InvalidTable("Conflicting Schemas", schema)





