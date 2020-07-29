import re


class Path:

    def __init__(self, path_str: str, protocal: str = "local"):
        self.protocal = protocal
        self.path_str = path_str
        
    def clean_path_str(self) -> str:
        return re.sub(r'(?<=[^:])\/\/', '/', self.path_str)

    def full_path(self) -> str:
        return "://".join([self.protocal, self.path_str])
