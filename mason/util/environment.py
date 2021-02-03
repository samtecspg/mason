import os
import sys
from typing import Optional
from os import path
from click._compat import iteritems
from dotenv import load_dotenv

from mason.definitions import from_root
from mason.state.local import LocalStateStore

from mason.util.printer import banner

class MasonEnvironment:
    def __init__(self,
                mason_home: Optional[str] = None,
             operator_module: Optional[str] = None, #TODO: get rid of these two
             workflow_module: Optional[str] = None,
             validation_path: Optional[str] = None
        ):
        self.mason_home = get_mason_home(mason_home)
        self.operator_module: str = operator_module or "operators"
        self.workflow_module: str = workflow_module or "workflows"
        self.validation_path: str = validation_path or from_root("/validations/")

        self.state_store = LocalStateStore(self.mason_home, get_client_version())

        self.load_environment_variables()

    def initialize(self) -> 'MasonEnvironment':
        self.state_store.initialize()
        return self

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

def get_mason_home(home: Optional[str]) -> str:
    return home or os.environ.get('MASON_HOME') or os.path.join(os.path.expanduser('~'), '.mason/')

def get_client_version() -> Optional[str]:
    try:
        import pkg_resources
        ver: Optional[str] = None
        module = sys._getframe(1).f_globals.get("__name__")
        for dist in pkg_resources.working_set:
            scripts = dist.get_entry_map().get("console_scripts") or {}
            for _, entry_point in iteritems(scripts):
                if entry_point.module_name == module:
                    ver = dist.version
        return ver
    except Exception as e:
        banner("Could not find client version", "fatal")
        return None

