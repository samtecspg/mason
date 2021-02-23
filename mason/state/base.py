from abc import abstractmethod
from dataclasses import dataclass
from typing import Optional, Union

@dataclass
class FailedOperation:
    message: str

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
    def cp_source(self, source: Optional[str], type: str, namespace: Optional[str] = "", command: Optional[str] = "", overwrite: bool = False) -> Union[str, FailedOperation]:
        raise Exception("Config cp_source not configured")

    @abstractmethod
    def set_session_config(self, config_id: str):
        raise Exception("Config set_session_config not configured")

    @abstractmethod
    def get_session_config(self) -> Optional[str]:
        raise Exception("Config get_session_config not configured")

