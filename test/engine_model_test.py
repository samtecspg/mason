from clients.response import Response
from definitions import from_root
from engines.metastore.models.schemas import from_file
from engines.metastore.models.schemas.parquet_schema import ParquetSchema, ParquetElement
from fsspec.implementations.local import LocalFileSystem # type: ignore

from util.logger import logger


class TestEngineModel():

    def test_parquet_schema_equality(self):
        columns1 = [
            ParquetElement("test_name", "test_type", "converted_type", "repitition_type"),
            ParquetElement("test_name_2", "test_type_2", "converted_type_2", "repitition_type_2")
        ]

        columns2 = [
            ParquetElement("test_name", "test_type", "converted_type", "repitition_type"),
            ParquetElement("test_name_3", "test_type_3", "converted_type_3", "repitition_type_2")
        ]

        schema1 = ParquetSchema(columns1)
        schema2 = ParquetSchema(columns1)
        schema3 = ParquetSchema(columns2)

        assert(schema1.__eq__(schema2))
        assert(not schema1.__eq__(schema3))
        assert(not schema2.__eq__(schema3))

        s = set([schema1, schema2, schema3])
        assert(len(s) == 2)

    def test_snappy_parquet_schema_support(self):
        logger.set_level("info")
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample.snappy.parquet')) as f:
            response, schema = from_file(f, Response())
            assert(schema.__class__.__name__ == "ParquetSchema")
            assert(response.formatted() == {'Errors': [], 'Info': [], 'Warnings': []})

    def test_file_not_supproted(self):
        logger.set_level("info")
        fs = LocalFileSystem()
        with fs.open(from_root('/test/unsupported_file_type.usf')) as f:
            response, schema = from_file(f, Response())
            expect = {'Errors': [], 'Info': [], 'Warnings': ['File type not supported for file /Users/kyle/dev/mason/test/unsupported_file_type.usf']}
            assert(response.formatted() == expect)
            assert(schema.__class__.__name__ == "MetastoreSchema")
