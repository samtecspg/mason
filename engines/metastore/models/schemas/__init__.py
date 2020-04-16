from typing import Union

import magic #type: ignore
from s3fs import S3File #type: ignore

from clients.response import Response
from engines.metastore.models.schemas.metastore_schema import emptySchema

from engines.metastore.models.schemas import parquet_schema as ParquetSchema
from engines.metastore.models.schemas import text_schema as TextSchema
from engines.metastore.models.schemas.metastore_schema import MetastoreSchema
from fsspec.spec import AbstractBufferedFile  # type: ignore


def from_file(file: AbstractBufferedFile, response: Response, read_headers: bool = True):
    header_size = 1000
    header = file.read(header_size)
    file_type = magic.from_buffer(header)

    if file_type == "Apache Parquet":
        return response, ParquetSchema.from_file(file)
    elif file_type == "CSV text":
        # header = file.readuntil('\n')
        return response, TextSchema.from_file(get_name(file), "csv", read_headers, response)
    elif file_type == "ASCII text":
        # header = file.readuntil('\n')
        return response, TextSchema.from_file(get_name(file), "none", read_headers, response)
    else:
        name = get_name(file)
        response.add_warning(f"File type not supported for file {name}")
        return response, emptySchema()

def get_name(file: Union[S3File, AbstractBufferedFile]):
    if file.__class__.__name__ == "S3File":
        name = "s3://" + file.path
    else:
        name = file.name
    return name

