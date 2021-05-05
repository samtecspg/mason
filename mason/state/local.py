import shutil
import os
from os import path
from typing import Optional, Union

from mason.state.base import MasonStateStore, FailedOperation
from mason.util.logger import logger
from mason.util.printer import banner
from pathlib import Path

class LocalStateStore(MasonStateStore):

    def get_home(self, type: str) -> Optional[str]:
        destinations = {
            "config": self.config_home,
            "workflow": self.workflow_home,
            "operator": self.operator_home
        }
        return destinations.get(type)
    
    def write_operator_type(self, source: str, type: str, destination: str, namespace: str, command: str, overwrite: bool = False) -> Union[str, FailedOperation]:
        if not path.exists(destination):
            os.makedirs(destination)
            Path(destination + "__init__.py").touch()

        full_path = destination + f"{command}/"

        if path.exists(full_path):
            if overwrite:
                shutil.rmtree(full_path)
                os.makedirs(full_path)
                shutil.copy(source, full_path + f"{type}.yaml")
                shutil.copy(path.dirname(source) + "/__init__.py", full_path + "__init__.py")
                return f"Overwrote definition for {type.capitalize()} {namespace}:{command}"
            else:
                return FailedOperation(f"Definition already exists for {type.capitalize()} {namespace}:{command}")
        else:
            os.makedirs(full_path)
            shutil.copy(source, full_path + f"{type}.yaml")
            shutil.copy(path.dirname(source) + "/__init__.py", full_path + "__init__.py")
            return f"Successfully saved {type.capitalize()} {namespace}:{command}"

    def write_config(self, source, destination: str, config_id: str, overwrite: bool = False) -> Union[str, FailedOperation]:
        config_name = path.basename(source)

        if not path.exists(destination):
            os.makedirs(destination)

        config_destination = destination + config_name
        if path.exists(config_destination):
            if overwrite:
                os.remove(config_destination)
                shutil.copy(source, destination + config_name)
                return f"Overwrote definition for Config: {config_id}"
            else:
                return FailedOperation(f"Config {config_id} already exists.")
        else:
            shutil.copy(source, destination + config_name)
            return f"Successfully saved Config {config_id}"

    # TODO: Copy to tmp instead of tracking source, and copy from tmp
    # TODO: Clean up type switches, serialize internal representation
    def cp_source(self, source: Optional[str], type: str, namespace: Optional[str] = None, command: Optional[str] = None, overwrite: bool = False) -> Union[str, FailedOperation]:
        namespace = namespace or ""
        command = command or ""
        if source:
            home = self.get_home(type)
            if home:
                if type == "config":
                    return self.write_config(source, home, namespace, overwrite)
                elif type in ["operator", "workflow"]:
                    tree_path = home + f"{namespace}/"
                    if command:
                        return self.write_operator_type(source, type, tree_path, namespace, command, overwrite)
                    else:
                        return FailedOperation(f"No command provided for {source}")
                else:
                    return FailedOperation(f"Type not supported: {type}")
            else:
                return FailedOperation(f"Unsupported type: {type}")
        else:
            return FailedOperation(f"Source path not found for {type}:{namespace}:{command}")

    def initialize(self) -> str:
        if not path.exists(self.home):
            banner(f"Creating MASON_HOME at {self.home}", "fatal")
            os.mkdir(self.home)
        if not path.exists(self.operator_home):
            banner(f"Creating OPERATOR_HOME at {self.operator_home}", "fatal")
            os.mkdir(self.operator_home)
            Path(self.operator_home + "__init__.py").touch()
        if not path.exists(self.workflow_home):
            banner(f"Creating WORKFLOW_HOME at {self.workflow_home}", "fatal")
            os.mkdir(self.workflow_home)
            Path(self.workflow_home + "__init__.py").touch()
        if not path.exists(self.config_home):
            banner(f"Creating CONFIG_HOME at {self.config_home}", "fatal")
            os.mkdir(self.config_home)

        return "Successfully initialized"

    def set_session_config(self, config_id: str):
        with open(self.config_home + "CURRENT_CONFIG", 'w+') as f:
            f.write(str(config_id))

    def get_session_config(self) -> Optional[str]:
        try:
            with open(self.config_home + "CURRENT_CONFIG", 'r') as f:
                return str(f.read())
        except FileNotFoundError as e:
            logger.warning("Current Mason config not set.  Run \"mason config -s <ID>\" to set session config.")
        return None


