from typing import Sequence, Optional, Union, List, Tuple

from pandas import DataFrame

from mason.engines.metastore.models.schemas.schema import SchemaElement, Schema, InvalidSchema
from mason.engines.storage.models.path import Path
from mason.util.exception import message
from io import StringIO


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

def df_from(text: StringIO, type: str, read_headers: Optional[bool]) -> Tuple[DataFrame, str]:
    headers = read_headers or False
    header: Optional[int] = None
    if headers:
        header = 0

    if (SUPPORTED_TYPES.get(type) == "csv-crlf"):
        line_terminator = "\r"
    else:
        line_terminator = "\n"
        
    # import great_expectations as ge
    # from great_expectations.dataset import PandasDataset
    # return ge.read_csv(text, lineterminator=line_terminator, header=header, dataset_class=PandasDataset), line_terminator
    import pandas as pd
    return pd.read_csv(text, lineterminator=line_terminator, header=header, skipinitialspace=True), line_terminator

def from_file(type: str, sample: bytes, path: Path, read_headers: Optional[bool]) -> Union[TextSchema, InvalidSchema]:
    text = StringIO(sample.decode("utf-8"))
    df, line_terminator = df_from(text, type, read_headers)
    
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





