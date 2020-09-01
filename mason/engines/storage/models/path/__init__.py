import re


class Path:

    def __init__(self, path_str: str, protocal: str = "file"):
        self.protocal = protocal
        self.path_str = path_str
        
    def clean_path_str(self) -> str:
        return re.sub(r'(?<=[^:])\/\/', '/', self.full_path())

    def full_path(self) -> str:
        if self.protocal != "file":
            return "://".join([self.protocal, self.path_str])
        else:
            return self.path_str
