import shutil
import os
from os import path
from typing import Optional

from mason.state.base import MasonStateStore
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
    
    def write_operator_type(self, source: str, type: str, destination: str, command: str, overwrite: bool = False):
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
                logger.info(f"Overwrote definition for {type}: {destination}{command}")
            else:
                logger.error(f"Definition already exists for {type}: {destination}{command}")
        else:
            os.makedirs(full_path)
            shutil.copy(source, full_path + f"{type}.yaml")
            shutil.copy(path.dirname(source) + "/__init__.py", full_path + "__init__.py")
            logger.info(f"Successfully saved {type}: {destination}{command}")

    def write_config(self, source, destination: str, overwrite: bool = False):
        config_name = path.basename(source)

        if not path.exists(destination):
            os.makedirs(destination)

        config_destination = destination + config_name
        if path.exists(config_destination):
            if overwrite:
                os.remove(config_destination)
                shutil.copy(source, destination + config_name)
                logger.info(f"Overwrote definition for config:{destination}")
            else:
                logger.error(f"Config {config_name} already exists.")
        else:
            shutil.copy(source, destination + config_name)
            logger.info(f"Succesfully saved config:{destination}")

    # TODO: Copy to tmp instead of tracking source, and copy from tmp
    # TODO: Clean up type switches, serialize internal representation
    def cp_source(self, source: Optional[str], type: str, namespace: Optional[str] = "", command: Optional[str] = "", overwrite: bool = False) -> Optional[str]:
        if source:
            home = self.get_home(type)
            if home:
                if type == "config":
                    self.write_config(source, home, overwrite)
                elif type in ["operator", "workflow"]:
                    tree_path = home + f"{namespace}/"
                    if command:
                        self.write_operator_type(source, type, tree_path, command, overwrite)
                        return f"Successfully copied {source} to {tree_path}"
                    else:
                        logger.error(f"No command provided for {source}")
                else:
                    logger.error(f"Type not supported: {type}")
            else:
                logger.error(f"Unsupported type: {type}")
        else:
            logger.error(f"Source path not found for {type}:{namespace}:{command}")
        return None

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

        return "Succesfully initialized"

    def set_session_config(self, config_id: str):
        with open(self.config_home + "CURRENT_CONFIG", 'w+') as f:
            f.write(str(config_id))

    def get_session_config(self) -> Optional[str]:
        try:
            with open(self.config_home + "CURRENT_CONFIG", 'r') as f:
                return str(f.read())
        except FileNotFoundError as e:
            logger.warning("Current Mason config not set.  Run \"mason run config <ID>\" to set session config.")
        return None


