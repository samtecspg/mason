import os
from typing import Optional
from util.logger import logger

def get_mason_home() -> str:
    try:
        return os.environ['MASON_HOME']
    except KeyError:
        return os.path.join(os.path.expanduser('~'), '.mason/')

class MasonEnvironment:
    def __init__(self, mason_home: Optional[str] = None, config_home: Optional[str] = None, operator_home: Optional[str] = None, operator_module: Optional[str] = None):
        self.mason_home: str = mason_home or get_mason_home()
        self.config_home: str = config_home or (self.mason_home + "config.yaml")
        self.operator_home: str = operator_home or (self.mason_home + "registered_operators/")
        self.operator_module = operator_module or "registered_operators"

