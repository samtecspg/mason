from typing import Sequence, Optional, Union, List
from tableschema import Table
from tabulator import FormatError

from mason.engines.metastore.models.schemas.schema import SchemaElement, Schema, InvalidSchema, InvalidSchemaElement
from mason.util.list import sequence
from mason.util.exception import message


class TextElement(SchemaElement):

    def __init__(self, name: str, type: str):
        self.name = name
        self.type = type
        super().__init__(name, type)

    def __eq__(self, other):
        if other:
            return (self.name == other.name and self.type == other.type)
        else:
            False

    def __hash__(self):
        return 0

class TextSchema(Schema):

    def __init__(self, columns: Sequence[TextElement], type: str):
        self.columns = columns
        if type == "none":
            self.type = 'text'
        else:
            self.type = 'text' + "-" + type


def from_file(file_name: str, type: str, header_length: int, read_headers: Optional[str]) -> Union[TextSchema, InvalidSchema]:
    headers = read_headers or False
    header_list: Union[List[str], bool]

    if (headers == False):
        header_list = list(map(lambda i: f"col_{i}", list(range(header_length))))
    else:
        header_list = True

    table = Table(file_name, headers=header_list)
    try:
        table.infer()
        fields = table.schema.descriptor.get('fields')

        def get_element(f: dict) -> Union[TextElement, InvalidSchemaElement]:
            name = f.get('name')
            type = f.get('type')
            if name and type:
                return TextElement(name, type)
            else:
                return InvalidSchemaElement(f"Schema name or type not found: {f}")

        valid, invalid = sequence(list(map(lambda f: get_element(f), fields)), TextElement, InvalidSchemaElement)
        errors = list(map(lambda e: InvalidSchemaElement(str(e) + f" File: {file_name}"), table.schema.errors))

        all_errors = invalid + errors
        all_error_messages = " ,".join(list(map(lambda e: e.reason, all_errors)))

        if len(invalid) > 0:
            return InvalidSchema(f"Table Parse errors: {all_error_messages}")
        elif len(valid) > 0:
            return TextSchema(valid, type)
        else:
            return InvalidSchema("No valid table elements")

    except FormatError as e:
            return InvalidSchema(f'File type not supported for file {file_name} {message(e)}')





