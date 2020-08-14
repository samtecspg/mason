import re


class Path:

    def __init__(self, path_str: str):
        self.path_str = path_str
        
    def clean_path_str(self) -> str:
        return re.sub(r'(?<=[^:])\/\/', '/', self.path_str)

