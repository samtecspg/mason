from typing import Union

import magic #type: ignore
from s3fs import S3File #type: ignore

from clients.response import Response
from engines.metastore.models.schemas.metastore_schema import emptySchema

from engines.metastore.models.schemas import parquet as ParquetSchema
from engines.metastore.models.schemas import json as JsonSchema
from engines.metastore.models.schemas import text as TextSchema
from engines.metastore.models.schemas.metastore_schema import MetastoreSchema
from fsspec.spec import AbstractBufferedFile  # type: ignore


def from_file(file: AbstractBufferedFile, response: Response, options: dict = {}):
    header_size = 1000
    header = file.read(header_size)
    file_type = magic.from_buffer(header)

    if file_type == "Apache Parquet":
        return response, ParquetSchema.from_file(file)
    elif file_type == "JSON data":
        return JsonSchema.from_file(get_name(file), response)
    elif file_type == "CSV text":
        # header = file.readuntil('\n')
        return TextSchema.from_file(get_name(file), "csv", response, options.get("read_headers"))
    elif file_type == "ASCII text":
        # header = file.readuntil('\n')
        return TextSchema.from_file(get_name(file), "none", response, options.get("read_headers"))
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

