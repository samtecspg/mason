from typing import Sequence, Tuple, Optional, Union, List

from tabulator import FormatError

from clients.response import Response
from engines.metastore.models.schemas.metastore_schema import MetastoreSchema, SchemaElement, emptySchema

from tableschema import Table

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

class TextSchema(MetastoreSchema):

    def __init__(self, columns: Sequence[TextElement], type: str):
        self.columns = columns
        if type == "none":
            self.type = 'text'
        else:
            self.type = 'text' + "-" + type


def from_file(file_name: str, type: str, response: Response, header_length: int, read_headers: Optional[str]) -> Tuple[Response, TextSchema]:
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
        columns = list(map(lambda f: TextElement(f.get('name'), f.get('type')), fields))

        errors = table.schema.errors
        for e in errors:
            response.add_error(str(e) + f" File: {file_name}")

        return response, TextSchema(columns, type)
    except FormatError as e:
        response.add_warning(f'File type not supported for file {file_name}')
        return response, emptySchema()





