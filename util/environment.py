import os
from typing import Optional
from definitions import from_root
from os import path
from util.printer import banner
from pathlib import Path

class MasonEnvironment:
    def __init__(self, mason_home: Optional[str] = None, config_home: Optional[str] = None, operator_home: Optional[str] = None, operator_module: Optional[str] = None):
        self.mason_home: str = mason_home or get_mason_home()
        self.config_home: str = config_home or (self.mason_home + "configurations/")
        self.operator_home: str = operator_home or (self.mason_home + "registered_operators/")
        self.operator_module = operator_module or "registered_operators"
        self.config_schema = from_root("/configurations/schema.json")

def get_mason_home() -> str:
    try:
        return os.environ['MASON_HOME']
    except KeyError:
        return os.path.join(os.path.expanduser('~'), '.mason/')

def initialize_environment(env: MasonEnvironment):
    if not path.exists(env.mason_home):
        banner(f"Creating MASON_HOME at {env.mason_home}", "fatal")
        os.mkdir(env.mason_home)
    if not path.exists(env.operator_home):
        banner(f"Creating OPERATOR_HOME at {env.operator_home}", "fatal")
        os.mkdir(env.operator_home)
        Path(env.operator_home + "__init__.py").touch()
    if not path.exists(env.config_home):
        banner(f"Creating CONFIG_HOME at {env.config_home}", "fatal")
        os.mkdir(env.config_home)


