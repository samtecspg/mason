# from fsspec.compression import SnappyFile
# from fsspec.spec import AbstractBufferedFile

from engines.metastore.models.schemas.parquet_schema import ParquetSchema, ParquetElement

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



