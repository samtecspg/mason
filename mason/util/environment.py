import os
import sys
from typing import Optional
from os import path

from click._compat import iteritems
from dotenv import load_dotenv
from pathlib import Path

from mason.definitions import from_root

from mason.util.printer import banner

class MasonEnvironment:
    def __init__(self,
             mason_home: Optional[str] = None,
             operator_module: Optional[str] = None, #TODO: get rid of these two
             workflow_module: Optional[str] = None,
             validation_path: Optional[str] = None
        ):
        self.client_version: Optional[str] = get_client_version()
        
        self.mason_home: str
        if self.client_version:
            self.mason_home = (mason_home or get_mason_home()) + f"{self.client_version}/" 
        else:
            self.mason_home = (mason_home or get_mason_home())
            
        self.config_home: str = self.mason_home + "configs/"
        self.operator_home: str = self.mason_home + "operators/"
        self.workflow_home: str = self.mason_home + "workflows/"
        self.operator_module: str = operator_module or "operators"
        self.workflow_module: str = workflow_module or "workflows"
        self.validation_path: str = validation_path or from_root("/validations/")

        self.load_environment_variables()


    def load_environment_variables(self):
        env_search_path = [
            self.mason_home + ".env",
            ".env",
            path.join(path.expanduser('~'), '.env'),
            path.join(path.expanduser('~'), '.mason_env')
        ]

        for p in env_search_path:
            if path.exists(p):
                load_dotenv(p)
                break

def get_mason_home() -> str:
    return os.environ.get('MASON_HOME') or os.path.join(os.path.expanduser('~'), '.mason/')

def get_client_version() -> Optional[str]:
    import pkg_resources
    ver: Optional[str] = None
    module = sys._getframe(1).f_globals.get("__name__")
    for dist in pkg_resources.working_set:
        scripts = dist.get_entry_map().get("console_scripts") or {}
        for _, entry_point in iteritems(scripts):
            if entry_point.module_name == module:
                ver = dist.version
    return ver
        

def initialize_environment(env: 'MasonEnvironment'):
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
