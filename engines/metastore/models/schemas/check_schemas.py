
from typing import List, Set, Tuple, Optional, Type, TypeVar, Union

from clients.response import Response
from engines.metastore.models.schemas.json import merge_schemas
from engines.metastore.models.schemas.schema import Schema
from functools import reduce
from util.list import get


M = TypeVar('M', bound=Schema)

def diff_schemas(schema1: M, schema2: M) -> M:
    column_diff = schema1.diff(schema2)
    #  NOTE: if two schemas differ in type all columns will differ by default
    #  Therefore choosing to take first schema's types arbitrarily
    # TODO: Look into why type isn't working here
    gen: M = Schema(list(column_diff), schema1.type) #type: ignore
    return gen


def find_conflicts(schemas: List[M], response: Response) -> Tuple[List[M], dict, Response]:

    working_schemas = schemas
    def get_name(s: Schema) -> Optional[str]:
        stype = s.type
        if not stype == "":
            return stype
        else:
            return None

    st = set(map(lambda s: get_name(s), schemas))
    schema_types = [x for x in st if x is not None]

    if len(schema_types) > 1:
        response.add_error("Mixed type schemas not supported at this time.  Ensure that files are of one type")
        return [], {'Schemas': []}, response
    elif schema_types == ['json']:
        # TODO: fix types here
        response, schema = merge_schemas(schemas, response) #type: ignore
        return [schema], {'Schema': schema.schema}, response #type: ignore
    elif schema_types == ['jsonl']:
        # TODO: fix types here
        response, schema = merge_schemas(schemas, response) #type: ignore
        return [schema], {'Schema': schema.schema}, response #type: ignore
    else:
        non_empty_schemas = filter(lambda s: s.columns != [], working_schemas)
        unique_schemas = set(non_empty_schemas)
        unique_schema_dicts = list(map(lambda s: s.to_dict(), list(unique_schemas)))

        if len(unique_schemas) > 1:

            diff = reduce(diff_schemas ,unique_schemas)

            data = {
                'SchemaConflicts': {
                    'CountDistinctSchemas': len(unique_schema_dicts),
                    'DistinctSchemas': unique_schema_dicts,
                    'NonOverlappingColumns': list(map(lambda s: {'name': s.name, 'type': s.type}, diff.columns))
                }
            }

            return list(unique_schemas), data, response
        else:
            return list(unique_schemas), { 'Schema': (get(list(unique_schema_dicts), 0) or []) }, response





