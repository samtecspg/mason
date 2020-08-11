from itertools import islice
from json import JSONDecodeError
from typing import List, Union
import json
import fsspec
import jsonlines
import os
from genson import SchemaBuilder

from mason.engines.metastore.models.schemas.schema import InvalidSchema, Schema
from mason.engines.metastore.models.schemas.schema import SchemaElement
from mason.engines.storage.models.path import Path
from mason.util.exception import message

def from_file(path: Path):
    file = path.full_path()
    # TODO: This code does not scale for large single json file (not jsonl)
    try:
        with fsspec.open(file, "r") as f:
            try: 
                data = [json.load(f)]
            except JSONDecodeError as e:
                f.seek(0)
                jsonl_preview = list(islice(f, 10))
                data = [json.loads(jline) for jline in jsonl_preview]
                json_type = "jsonl"
            builder = SchemaBuilder()
            for d in data:
                builder.add_object(d)
            schema = builder.to_schema()
            return JsonSchema(schema, path, json_type)
    except Exception as e:
        return InvalidSchema(f"File not found {file}")

class JsonSchema(Schema):

    def __init__(self, schema: dict, path: Path, type: str = "json"):
        self.schema = schema
        columns: List[SchemaElement] =  [] # TODO:  Treating json data as non tabular for now.   Surface main columns and nested attributes
        super().__init__(columns, type, path)

def merge_json_schemas(schemas: List[Schema]) -> Union[JsonSchema, InvalidSchema]:
    try:
        builder = SchemaBuilder()
        for schema in schemas:
            if isinstance(schema, JsonSchema):
                builder.add_schema(schema.schema)
            else:
                return InvalidSchema("merge_json_schemas Only supports JsonSchema type")
        merged = builder.to_schema()
        return JsonSchema(merged, [])
    except Exception as e:
        return InvalidSchema(f"Invalid Schema, builder error: {message(e)}")









