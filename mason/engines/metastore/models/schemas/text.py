from typing import Sequence, Optional, Union, List

from mason.engines.metastore.models.schemas.schema import SchemaElement, Schema, InvalidSchema, InvalidSchemaElement
from mason.engines.storage.models.path import Path
from mason.util.exception import message
import pandas as pd


SUPPORTED_TYPES = {
    "CSV text": "csv",
    "ASCII text, with CRLF, LF line terminators": "csv-crlf"
}

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

    def __init__(self, columns: Sequence[TextElement], type: str, path: Path, line_terminator: str = "\n"):
        self.inferred_type = type
        self.line_terminator = line_terminator
        super().__init__(columns, SUPPORTED_TYPES.get(type, ""), path)

def from_file(type: str, sample: bytes, path: Path, read_headers: Optional[bool]) -> Union[TextSchema, InvalidSchema]:
    headers = read_headers or False
    header_list: Union[List[str], bool]
    
    header: Optional[int] = None
    if headers:
        header = 0

    if (SUPPORTED_TYPES.get(type) == "csv-crlf"):
        line_terminator = "\r"
    else:
        line_terminator = "\n"

    from io import StringIO
    df = pd.read_csv(StringIO(sample.decode("utf-8")), lineterminator=line_terminator, header=header)
    
    try:

        fields = [(k, v.name) for k, v in dict(df.dtypes).items()]
        elements = list(map(lambda i: TextElement(*i), fields))

        if len(elements) > 0:
            if type in SUPPORTED_TYPES:
                return TextSchema(elements, type, path, line_terminator)
            else:
                return InvalidSchema(f"Unsupported text type: {type}")
        else:
            return InvalidSchema("No valid table elements")

    except Exception as e:
            return InvalidSchema(f'File type not supported for file {path.path_str} {message(e)}')





