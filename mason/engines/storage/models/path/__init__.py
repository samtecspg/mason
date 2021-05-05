import re
from typing import List, Optional

class Path:

    def __init__(self, path_str: str, protocol: str = "file"):
        self.protocol = protocol
        self.path_str = path_str
        
    def clean_path_str(self) -> str:
        return re.sub(r'(?<=[^:])\/\/', '/', self.full_path())

    def full_path(self) -> str:
        if self.protocol != "file":
            return "://".join([self.protocol, self.path_str])
        else:
            return self.path_str
        

def construct(parts: List[str], protocol: Optional[str] = None) -> Path:
    # TODO: use os path methods for this
    join = "/".join(parts)
    clean = re.sub(r'/+', '/', join)
    return Path(clean, protocol or "file")
    