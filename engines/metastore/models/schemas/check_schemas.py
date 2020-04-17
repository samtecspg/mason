
from typing import List, Set, Tuple, Optional

from clients.response import Response
from engines.metastore.models.schemas.json import merge_schemas
from engines.metastore.models.schemas.metastore_schema import MetastoreSchema
from util.list import get

def find_conflicts(schemas: List[MetastoreSchema], response: Response) -> Tuple[List[MetastoreSchema], dict, Response]:

    working_schemas = schemas
    def get_name(s: MetastoreSchema) -> Optional[str]:
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
            cummulative_intersection: Set[str] = set()
            cummulative_union: Set[str] = set()
            last_schema: Set[str] = set(map(lambda s: s.name, working_schemas.pop().columns))

            for schema in unique_schemas:
                while len(working_schemas) > 0:
                    ## Note:  Only comparing names at this time

                    children_names: Set[str] = set(map(lambda s: s.name, schema.columns))
                    int: Set[str] = last_schema.intersection(children_names)

                    cummulative_intersection = cummulative_intersection.intersection(set(int))
                    cummulative_union = cummulative_union.union(set(children_names))

                    children = working_schemas.pop().columns
                    last_schema = set(map(lambda s: s.name, children))

            complement_of_intersection = list(cummulative_union.difference(cummulative_intersection))
            complement_of_intersection.sort()
            data = {
                'SchemaConflicts': {
                    'CountDistinctSchemas': len(unique_schema_dicts),
                    'DistinctSchemas': unique_schema_dicts,
                    'NonOverlappingColumns': complement_of_intersection
                }
            }

            return list(unique_schemas), data, response
        else:
            return list(unique_schemas), { 'Schema': (get(list(unique_schema_dicts), 0) or []) }, response





