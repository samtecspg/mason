import magic #type: ignore

from clients.response import Response
from engines.metastore.models.schemas.metastore_schema import emptySchema

from engines.metastore.models.schemas import parquet_schema as ParquetSchema
from engines.metastore.models.schemas.metastore_schema import MetastoreSchema
from fsspec.spec import AbstractBufferedFile # type: ignore

def from_file(file: AbstractBufferedFile, response: Response):
    header_size = 1000
    header = file.read(header_size)
    file_type = magic.from_buffer(header)
    if file_type == "Apache Parquet":
        return response, ParquetSchema.from_file(file)
    else:
        response.add_warning(f"File type not supported for file {file.name}")
        return response, emptySchema()

