from mason.engines.metastore.models.schemas.check_schemas import find_conflicts
from mason.engines.metastore.models.schemas.parquet import ParquetElement, ParquetSchema
from mason.engines.metastore.models.schemas.schema import InvalidSchema, SchemaConflict
from mason.engines.metastore.models.schemas.schemas import from_file
from mason.engines.metastore.models.schemas.text import TextSchema

from mason.definitions import from_root
from mason.engines.metastore.models.schemas.json import from_file as json_from_file, JsonSchema
from fsspec.implementations.local import LocalFileSystem

from mason.util.logger import logger

class TestUnsupportedSchema:

    def test_file_not_supported(self):
        logger.set_level("error")
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/unsupported_file_type.usf')) as f:
            schema = from_file(f)
            assert(isinstance(schema, InvalidSchema))
            assert(schema.reason[0:32] == f"File type not supported for file")

class TestJSONSchema:

    def test_valid_json(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/json_simple.json')) as f:
            schema = from_file(f)
            assert(isinstance(schema, JsonSchema))
            expect = {'$schema': 'http://json-schema.org/schema#',
                      'properties': {'field': {'type': 'string'}, 'field2': {'type': 'string'}, 'field3': {'type': 'string'}},
                      'required': ['field', 'field2', 'field3'],
                      'type': 'object'}
            assert(schema.schema == expect)
            assert(schema.to_dict() == {'Columns': [], 'SchemaType': 'json'})
            assert(schema.to_pd_dict() == {})

    def test_invalid_json(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/bad_json.json')) as f:
            schema = from_file(f, {})
            assert(isinstance(schema, InvalidSchema))
            message = f"File type not supported for file {from_root('/test/sample_data/bad_json.json')}.  Type: ASCII text, with no line terminators"
            assert(message in schema.reason)

    def test_complex_json(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/complex_json.json')) as f:
            schema = from_file(f)
            assert(isinstance(schema, JsonSchema))
            expect = {'$schema': 'http://json-schema.org/schema#', 'type': 'object', 'properties': {'data': {'type': 'array', 'items': {'type': 'object','properties': {'field1': {'type': 'string'},'field2': {'type': ['integer', 'string']},'field3': {'type': 'string'},'field4': {'type': 'string'},'field5': {'type': 'object','properties': {'some_other_stuff': {'type': 'string'}},'required': ['some_other_stuff']}}}}}, 'required': ['data']}
            assert(schema.schema == expect)

    def test_jsonl(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/json_lines.jsonl')) as f:
            schema = from_file(f)
            assert(isinstance(schema, JsonSchema))
            expect = {'$schema': 'http://json-schema.org/schema#', 'properties': {'field': {'type': 'string'},'field2': {'type': 'string'},'field3': {'type': 'string'},'field4': {'type': 'string'},'field5': {'type': 'string'},'field6': {'type': 'string'},'field7': {'type': 'string'}}, 'type': 'object'}
            assert(schema.schema == expect)

    def test_file_dne(self):
        schema = json_from_file('test/sample_data/dne.json')
        assert (isinstance(schema, InvalidSchema))
        message = 'File not found test/sample_data/dne.json'
        assert(schema.reason[0:97] == message)

    def test_check_schemas(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/complex_json.json')) as f:
            schema1 = from_file(f)
            assert(isinstance(schema1, JsonSchema))

        with fs.open(from_root('/test/sample_data/complex_json_2.json')) as f:
            schema2 = from_file(f)
            assert(isinstance(schema2, JsonSchema))

        with fs.open(from_root('/test/sample_data/json_simple.json')) as f:
            schema3 = from_file(f)
            assert(isinstance(schema3, JsonSchema))

        with fs.open(from_root('/test/sample_data/unsupported_file_type.usf')) as f:
            schema4 = from_file(f)
            assert(isinstance(schema4, InvalidSchema))

        with fs.open(from_root('/test/sample_data/csv_sample.csv')) as f:
            schema5 = from_file(f, {"read_headers": True})
            assert(isinstance(schema5, TextSchema))

        with fs.open(from_root('/test/sample_data/json_lines.jsonl')) as f:
            schema6 = from_file(f)
            assert(isinstance(schema6, JsonSchema))


        with fs.open(from_root('/test/sample_data/json_lines2.jsonl')) as f:
            schema7 = from_file(f)
            assert(isinstance(schema7, JsonSchema))

        schema = find_conflicts([schema1, schema2])
        expect = {'$schema': 'http://json-schema.org/schema#','properties': {'data': {'items': {'properties': {'field1': {'type': 'string'},'field2': {'type': ['integer','string']},'field3': {'type': 'string'},'field4': {'type': 'string'},'field5': {'properties': {'some_other_stuff': {'type': 'string'}},'required': ['some_other_stuff'],'type': 'object'},'field6': {'type': 'string'}},'type': 'object'},'type': 'array'}},'required': ['data'],'type': 'object'}
        assert(isinstance(schema, JsonSchema))
        assert(schema.schema == expect)

        schema = find_conflicts([schema1, schema2, schema3])
        assert(isinstance(schema, JsonSchema))
        expect = {'$schema': 'http://json-schema.org/schema#', 'properties': {'data': {'items': {'properties': {'field1': {'type': 'string'},'field2': {'type': ['integer','string']},'field3': {'type': 'string'},'field4': {'type': 'string'},'field5': {'properties': {'some_other_stuff': {'type': 'string'}},'required': ['some_other_stuff'],'type': 'object'},'field6': {'type': 'string'}}, 'type': 'object'}, 'type': 'array'},'field': {'type': 'string'},'field2': {'type': 'string'},'field3': {'type': 'string'}}, 'required': [], 'type': 'object'}
        assert(schema.schema == expect)
        schema = find_conflicts([schema1, schema2, schema3,  schema5])
        assert(isinstance(schema, InvalidSchema))
        assert(schema.reason == "Mixed type schemas not supported at this time.  Ensure that files are of one type")

        schema = find_conflicts([schema6, schema7])
        assert(isinstance(schema, JsonSchema))
        expect = {'$schema': 'http://json-schema.org/schema#', 'properties': {'field': {'type': 'string'},'field2': {'type': 'string'},'field3': {'type': 'string'},'field4': {'type': 'string'},'field5': {'type': 'string'},'field6': {'type': 'string'},'field7': {'type': 'string'},'other': {'type': 'string'},'other2': {'type': 'string'},'other3': {'type': 'string'}}, 'required': ['other'], 'type': 'object'}
        assert(schema.schema == expect)

class TestTextSchema:

    def test_valid_csv(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/csv_sample.csv')) as f:
            schema = from_file(f, {"read_headers": True})
            assert(isinstance(schema, TextSchema))
            assert(list(map(lambda c: c.name,schema.columns)) == ["type","price"])
            assert(list(map(lambda c: c.type,schema.columns)) == ["string","number"])

    def test_valid_csv_crlf_lf(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/csv_crlf_sample.csv')) as f:
            schema = from_file(f, {"read_headers": True})
            assert(isinstance(schema, TextSchema))
            # assert(list(map(lambda c: c.name,schema.columns)) == ["type","price"])
            # assert(list(map(lambda c: c.type,schema.columns)) == ["string","number"])

    def test_csv_equality(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/csv_sample.csv')) as f:
            schema1 = from_file(f, {"read_headers": True})
            assert(isinstance(schema1, TextSchema))

        with fs.open(from_root('/test/sample_data/csv_sample_2.csv')) as f:
            schema2 = from_file(f, {"read_headers": True})
            assert(isinstance(schema2, TextSchema))

        schema = find_conflicts([schema1, schema2])
        assert(isinstance(schema, SchemaConflict))
        expect = {'CountDistinctSchemas': 2, 'DistinctSchemas': [{'SchemaType': 'text', 'Columns': [{'Name': 'type', 'Type': 'string'}, {'Name': 'price', 'Type': 'number'}]},{'SchemaType': 'text', 'Columns': [{'Name': 'type', 'Type': 'string'}, {'Name': 'price', 'Type': 'number'}, {'Name': 'availabile', 'Type': 'boolean'}, {'Name': 'date', 'Type': 'date'}]}], 'NonOverlappingColumns': [{'name': 'availabile', 'type': 'boolean'}, {'name': 'date', 'type': 'date'}]}
        assert(schema.to_dict() == {'SchemaConflicts': expect})


    def test_csv_no_header(self):
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/csv_no_header.csv')) as f:
            schema = from_file(f)
            assert(isinstance(schema, TextSchema))
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

        schema = find_conflicts([schema1, schema3])
        assert(isinstance(schema, SchemaConflict))

        assert(schema.to_dict()['SchemaConflicts']['NonOverlappingColumns'] == [{'name': 'test_name_2', 'type': 'test_type_2'}, {'name': 'test_name_3', 'type': 'test_type_3'}])

    def test_snappy_parquet_schema_support(self):
        logger.set_level("info")
        fs = LocalFileSystem()
        with fs.open(from_root('/test/sample_data/sample.snappy.parquet')) as f:
            schema = from_file(f)
            assert(isinstance(schema, ParquetSchema))

