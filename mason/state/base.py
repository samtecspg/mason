from abc import abstractmethod
from typing import Optional

class MasonStateStore:

    def __init__(self, home_path: str, client_version: Optional[str]):
        if client_version:
            self.client_version = client_version
            self.home = home_path + f"{client_version}/"
        else:
            self.home = home_path
            self.client_version = "no-version"
        self.operator_home = self.home + "operators/"
        self.config_home = self.home + "configs/"
        self.workflow_home = self.home + "workflows/"

    @abstractmethod
    def cp_source(self, source: Optional[str], type: str, postfix: str, overwrite: bool = False):
        raise Exception("Config save not configured")

    # @abstractmethod
    # def get_config(self, config_id: str):
    #     raise Exception("Config save not configured")
    #
    # @abstractmethod
    # def get_operator(self, namespace: str, operator: str):
    #     raise Exception("get_operator not configured for state store")
    #
    # @abstractmethod
    # def get_workflow(self, namespace: str, workflow: str):
    #     raise Exception("get_workflow not configured for state store")
