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
    
    def write_operator_type(self, source: str, type: str, destination: str, overwrite: bool = False):
        os.makedirs(destination)
        if path.exists(destination):
            if overwrite:
                shutil.rmtree(destination)
                shutil.copy(source, destination + f"{type}.yaml")
                shutil.copy(path.dirname(source), destination + f"{type}.yaml")
            else:
                logger.error(f"Definition already exists for {type}")
        else:
            shutil.copy(source, destination + f"{type}.yaml")
            shutil.copy(path.dirname(source), destination + f"{type}.yaml")
            
    def write_config(self, source, destination: str, overwrite: bool = False):
        config_name = path.basename(source)
        config_destination = destination + config_name
        if path.exists(config_destination):
            if overwrite:
                shutil.rmtree(config_destination)
                shutil.copy(source, destination + config_name)
            else:
                logger.error(f"Config {config_name} already exists.")
        else:
            shutil.copy(source, destination + config_name)
        
    # TODO: Copy to tmp instead of tracking source, and copy from tmp
    def cp_source(self, source: Optional[str], type: str, postfix: str = "", overwrite: bool = False):
        if source:
            home = self.get_home(type)
            if home:
                tree_path = ("/").join([home.rstrip("/"), postfix])
                if type == "config":
                    self.write_config(source, tree_path, overwrite)
                elif type in ["operator", "workflow"]:
                    self.write_operator_type(source, type, tree_path, overwrite)
                else:
                    logger.error(f"Type not supported: {type}")
        else:
            logger.error(f"Source path not found for {type}:{postfix}")

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

    def get_config(self, config_id: str):
            pass

    def get_operator(self, namespace: str, operator: str):
        pass

    def get_workflow(self, namespace: str, workflow: str):
        pass

