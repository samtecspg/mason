from clients.response import Response
from definitions import from_root
from engines.metastore.models.schemas import from_file
from engines.metastore.models.schemas.check_schemas import find_conflicts
from engines.metastore.models.schemas.json import from_file as json_from_file
from engines.metastore.models.schemas.parquet import ParquetSchema, ParquetElement
from fsspec.implementations.local import LocalFileSystem # type: ignore

from util.logger import logger

class TestUnsupportedSchema:

    def test_file_not_supported(self):
        logger.set_level("error")
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/unsupported_file_type.usf')) as f:
            response, schema = from_file(f, Response())
            expect = {'Errors': [], 'Info': [], 'Warnings': ['File type not supported for file /Users/kyle/dev/mason/test/sample_data/unsupported_file_type.usf']}
            assert(response.formatted() == expect)
            assert(schema.__class__.__name__ == "MetastoreSchema")

class TestJSONSchema:

    def test_valid_json(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/json_simple.json')) as f:
            response, schema = from_file(f, Response())
            assert(schema.__class__.__name__ == "JsonSchema")
            expect = {'$schema': 'http://json-schema.org/schema#',
                      'properties': {'field': {'type': 'string'}, 'field2': {'type': 'string'}, 'field3': {'type': 'string'}},
                      'required': ['field', 'field2', 'field3'],
                      'type': 'object'}
            assert(schema.schema == expect)
            assert(response.formatted() == {'Errors': [], 'Info': [], 'Warnings': []})

    def test_invalid_json(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/bad_json.json')) as f:
            response, schema = from_file(f, Response())
            assert(schema.__class__.__name__ == "MetastoreSchema")
            expect = {'Errors': [], 'Info': [], 'Warnings': ['File type not supported for file /Users/kyle/dev/mason/test/sample_data/bad_json.json']}
            assert(response.formatted() == expect)

    def test_complex_json(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/complex_json.json')) as f:
            response, schema = from_file(f, Response())
            assert(schema.__class__.__name__ == "JsonSchema")
            expect = {'$schema': 'http://json-schema.org/schema#', 'type': 'object', 'properties': {'data': {'type': 'array', 'items': {'type': 'object','properties': {'field1': {'type': 'string'},'field2': {'type': ['integer', 'string']},'field3': {'type': 'string'},'field4': {'type': 'string'},'field5': {'type': 'object','properties': {'some_other_stuff': {'type': 'string'}},'required': ['some_other_stuff']}}}}}, 'required': ['data']}
            assert(schema.schema == expect)
            expect = {'Errors': [], 'Info': [], 'Warnings': []}

    def test_jsonl(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/json_lines.jsonl')) as f:
            response, schema = from_file(f, Response())
            assert(schema.__class__.__name__ == "JsonSchema")
            expect = {'$schema': 'http://json-schema.org/schema#', 'properties': {'field': {'type': 'string'},'field2': {'type': 'string'},'field3': {'type': 'string'},'field4': {'type': 'string'},'field5': {'type': 'string'},'field6': {'type': 'string'},'field7': {'type': 'string'}}, 'type': 'object'}
            assert(schema.schema == expect)
            expect = {'Errors': [], 'Info': [], 'Warnings': []}

    def test_file_dne(self):
        response, schema = json_from_file('test/sample_data/dne.json', Response())
        assert(schema.__class__.__name__ == "MetastoreSchema")
        expect = {'Errors': ['File not found test/sample_data/dne.json'], 'Info': [], 'Warnings': []}
        assert (response.formatted() == expect)

    def test_check_schemas(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/complex_json.json')) as f:
            response, schema1 = from_file(f, Response())

        with fs.open(from_root('/test/sample_data/complex_json_2.json')) as f:
            response, schema2 = from_file(f, Response())

        with fs.open(from_root('/test/sample_data/json_simple.json')) as f:
            response, schema3 = from_file(f, Response())

        with fs.open(from_root('/test/sample_data/unsupported_file_type.usf')) as f:
            response, schema4 = from_file(f, Response())

        with fs.open(from_root('/test/sample_data/csv_sample.csv')) as f:
            response, schema5 = from_file(f, Response(), {"read_headers": True})

        with fs.open(from_root('/test/sample_data/json_lines.jsonl')) as f:
            response, schema6 = from_file(f, Response())

        with fs.open(from_root('/test/sample_data/json_lines2.jsonl')) as f:
            response, schema7 = from_file(f, Response())

        schema, data, response = find_conflicts([schema1, schema2], response)
        expect = {'$schema': 'http://json-schema.org/schema#','properties': {'data': {'items': {'properties': {'field1': {'type': 'string'},'field2': {'type': ['integer','string']},'field3': {'type': 'string'},'field4': {'type': 'string'},'field5': {'properties': {'some_other_stuff': {'type': 'string'}},'required': ['some_other_stuff'],'type': 'object'},'field6': {'type': 'string'}},'type': 'object'},'type': 'array'}},'required': ['data'],'type': 'object'}
        assert(schema[0].schema == expect)

        schema, data, response = find_conflicts([schema1, schema2, schema3], response)
        expect = {'$schema': 'http://json-schema.org/schema#', 'properties': {'data': {'items': {'properties': {'field1': {'type': 'string'},'field2': {'type': ['integer','string']},'field3': {'type': 'string'},'field4': {'type': 'string'},'field5': {'properties': {'some_other_stuff': {'type': 'string'}},'required': ['some_other_stuff'],'type': 'object'},'field6': {'type': 'string'}}, 'type': 'object'}, 'type': 'array'},'field': {'type': 'string'},'field2': {'type': 'string'},'field3': {'type': 'string'}}, 'required': [], 'type': 'object'}
        assert(schema[0].schema == expect)

        schema, data, response = find_conflicts([schema1, schema2, schema3, schema4, schema5], response)
        assert(schema == [])
        assert(data == {'Schemas': []})
        assert(response.formatted() == {'Errors': ['Mixed type schemas not supported at this time.  Ensure that files are of one type'], 'Info': [], 'Warnings': []})

        schema, data, response = find_conflicts([schema6, schema7], response)
        expect = {'$schema': 'http://json-schema.org/schema#', 'properties': {'field': {'type': 'string'},'field2': {'type': 'string'},'field3': {'type': 'string'},'field4': {'type': 'string'},'field5': {'type': 'string'},'field6': {'type': 'string'},'field7': {'type': 'string'},'other': {'type': 'string'},'other2': {'type': 'string'},'other3': {'type': 'string'}}, 'required': ['other'], 'type': 'object'}
        assert(schema[0].schema == expect)

class TestTextSchema:

    def test_valid_csv(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/csv_sample.csv')) as f:
            response, schema = from_file(f, Response(), {"read_headers": True})
            assert(schema.__class__.__name__ == "TextSchema")
            assert(response.formatted() == {'Errors': [], 'Info': [], 'Warnings': []})
            assert(list(map(lambda c: c.name,schema.columns)) == ["type","price"])
            assert(list(map(lambda c: c.type,schema.columns)) == ["string","number"])

    def test_csv_no_header(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/csv_no_header.csv')) as f:
            response, schema = from_file(f, Response())
            assert(schema.__class__.__name__ == "TextSchema")
            assert(response.formatted() == {'Errors': [], 'Info': [], 'Warnings': []})
            assert(list(map(lambda c: c.name,schema.columns)) == ["col_0","col_1"])
            assert(list(map(lambda c: c.type,schema.columns)) == ["string","number"])

class TestParquetSchema:

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
        with fs.open(from_root('/test/sample_data/sample.snappy.parquet')) as f:
            response, schema = from_file(f, Response())
            assert(schema.__class__.__name__ == "ParquetSchema")
            assert(response.formatted() == {'Errors': [], 'Info': [], 'Warnings': []})

