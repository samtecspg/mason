import os
from typing import Optional, Dict

from dotenv import load_dotenv

from os import path
from mason.util.printer import banner
from pathlib import Path


class MasonEnvironment:
    def __init__(self,
             mason_home: Optional[str] = None,
             config_home: Optional[str] = None,
             operator_home: Optional[str] = None,
             operator_module: Optional[str] = None,
             workflow_home: Optional[str] = None,
             workflow_module: Optional[str] = None,
    ):
        self.mason_home: str = mason_home or get_mason_home()
        self.config_home: str = config_home or (self.mason_home + "configurations/")
        self.operator_home: str = operator_home or (self.mason_home + "registered_operators/")
        self.workflow_home: str = workflow_home or (self.mason_home + "registered_workflows/")
        self.operator_module = operator_module or "registered_operators"
        self.workflow_module = workflow_module or "registered_workflows"

        load_dotenv(self.mason_home + ".env")

def get_mason_home() -> str:
    return os.environ.get('MASON_HOME') or os.path.join(os.path.expanduser('~'), '.mason/')

def initialize_environment(env: MasonEnvironment):
    if not path.exists(env.mason_home):
        banner(f"Creating MASON_HOME at {env.mason_home}", "fatal")
        os.mkdir(env.mason_home)
    if not path.exists(env.operator_home):
        banner(f"Creating OPERATOR_HOME at {env.operator_home}", "fatal")
        os.mkdir(env.operator_home)
        Path(env.operator_home + "__init__.py").touch()
    if not path.exists(env.workflow_home):
        banner(f"Creating WORKFLOW_HOME at {env.workflow_home}", "fatal")
        os.mkdir(env.workflow_home)
        Path(env.operator_home + "__init__.py").touch()
    if not path.exists(env.config_home):
        banner(f"Creating CONFIG_HOME at {env.config_home}", "fatal")
        os.mkdir(env.config_home)




