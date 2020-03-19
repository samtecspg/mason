

from typing import List, Set
from engines.metastore.models.schemas.metastore_schema import MetastoreSchema, SchemaElement
from util.list import get

def find_conflicts(schemas: List[MetastoreSchema]):
    working_schemas = schemas
    unique_schemas = set(schemas)
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

        complement_of_intersection = cummulative_union.difference(cummulative_intersection)

        return {
            'SchemaConflicts': {
                'Distinct Schemas': unique_schema_dicts,
                'Non-overlapping Columns': list(complement_of_intersection)
            }
        }
    else:
        return {
            'Schema': (get(list(unique_schema_dicts), 0) or [])
        }





