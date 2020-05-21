from typing import Union

import magic
from s3fs import S3File

from clients.response import Response
from engines.metastore.models.schemas.schema import emptySchema

from engines.metastore.models.schemas import parquet as ParquetSchema
from engines.metastore.models.schemas import json as JsonSchema
from engines.metastore.models.schemas import text as TextSchema
from engines.metastore.models.schemas.schema import Schema
from fsspec.spec import AbstractBufferedFile

def header_length(file: AbstractBufferedFile):
    head = []
    for i in range(4):
        head.append(file.readline().decode("utf-8"))
    file.seek(0)
    header_length = max(list(map(lambda l: len(l.split(",")), head)))
    return header_length


def from_file(file: AbstractBufferedFile, response: Response, options: dict = {}):
    header_size = 1000
    header = file.read(header_size)
    file_type = magic.from_buffer(header)
    file.seek(0)

    if file_type == "Apache Parquet":
        return response, ParquetSchema.from_file(file)
    elif file_type == "JSON data":
        return JsonSchema.from_file(get_name(file), response)
    elif file_type == "CSV text" or file_type == "ASCII text":
        return TextSchema.from_file(get_name(file), "none", response, header_length(file), options.get("read_headers"))
    else:
        name = get_name(file)
        response.add_warning(f"File type not supported for file {name}")
        return response, emptySchema()

def get_name(file: Union[S3File, AbstractBufferedFile]) -> str:
    if file.__class__.__name__ == "S3File":
        name = "s3://" + file.path
    else:
        name = file.name
    return name

