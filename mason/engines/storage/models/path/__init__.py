import re
from typing import Optional, Union
from urllib.parse import urlsplit, urlunparse
from os.path import splitext

class Path:

    def __init__(self, path_str: str, protocol: str = "file", database_name: Optional[str] = None, table_name: Optional[str] = None):
        self.protocol = protocol
        self.path_str = path_str
        self.database_name: Optional[str] = database_name
        self.table_name: Optional[str] = table_name
        
    def display_table_name(self) -> Optional[str]:
        if self.table_name:
            return splitext(self.table_name.split("/")[-1])[0]
        else:
            return None

    def clean_path_str(self) -> str:
        return re.sub(r'(?<=[^:])\/\/', '/', self.full_path())

    def full_path(self) -> str:
        if self.database_name and self.table_name:
            return urlunparse((self.protocol, self.database_name, self.table_name, "", "", "")) 
        else:
            clean_protocol = re.sub("^.+://", "", self.path_str)
            return urlunparse((self.protocol, clean_protocol, "", "", "", ""))

class InvalidPath:
    
    def __init__(self, path_str: str, reason: str):
        self.path_str = path_str
        self.reason = reason

    def message(self) -> str:
        format_message = "Format for input_paths: <PROTOCOL>://<DATABASE/BUCKET_NAME>/<TABLE_NAME/PATH>"
        return self.reason + " " + format_message

def cast_to_opt(s: str) -> Optional[str]:
    if s == '':
        return None
    else:
        return s

def parse_path(protocol_str: str, protocol: str) -> Union[Path, InvalidPath]:
    parsed = urlsplit(protocol_str, allow_fragments=False)
    if not cast_to_opt(parsed.scheme):
        protocol_str = "://".join([protocol, protocol_str.replace("://", "")])
        parsed = urlsplit(protocol_str, allow_fragments=False)

    protocol_parsed = cast_to_opt(parsed[0])
    database_name = cast_to_opt(parsed[1])
    table_name = cast_to_opt(parsed[2]) 
    
    if protocol_parsed and protocol_parsed != protocol:
        return InvalidPath(protocol_str, f"Specified protocol for path does not match engine: {protocol_parsed} != {protocol}.")
            
    return Path(protocol_str, protocol, database_name, table_name)

def parse_database_path(database_path: str, protocol: str) -> Union[Path, InvalidPath]:
    path = parse_path(database_path, protocol)
    if isinstance(path, Path):
        if path.database_name:
            return path
        else:
            return InvalidPath(database_path, "Database not specified")
    else:
        return path

def parse_table_path(table_path: str, protocol: str) -> Union[Path, InvalidPath]:
    path = parse_database_path(table_path, protocol)
    if isinstance(path, Path):
        if path.table_name:
            return path
        else:
            return InvalidPath(table_path, "Table not specified")
    else:
        return path
