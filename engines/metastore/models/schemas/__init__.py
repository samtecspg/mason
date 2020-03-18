import tempfile
import magic #type: ignore
from typing import Optional
from engines.metastore.models.schemas import parquet_schema as ParquetSchema

def from_header_and_footer(header: bytes, footer: bytes):
    with tempfile.NamedTemporaryFile(delete=False) as header_file:
        header_file.write(header)
        file_type: Optional[str] = magic.from_file(header_file.name)

    with tempfile.NamedTemporaryFile(delete=False) as footer_file:
        footer_file.write(footer)

        if file_type == "Apache Parquet":
            return ParquetSchema.from_footer(footer_file.name)



