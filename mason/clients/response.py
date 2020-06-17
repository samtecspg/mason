from datetime import datetime
from typing import List, Tuple, Optional

from mason.util.logger import logger

#  This class is only intended to 
class Response:
    def __init__(self):
        self.responses: List[dict] = []
        self.warnings: List[str] = []
        self.info: List[str] = []
        self.errors: List[str] = []
        self.configs = []
        self.current_config = None
        self.status_code: int = 200
        self.data: List[dict] = []

    def add_timestamp(self, s: Optional[str] = None):
        if s:
            return f"{datetime.now()}: {s}"
        else:
            return f"{datetime.now()}"
    
    def errored(self):
        return not (len(self.errors) == 0)

    def add_warning(self, warning: str, log: bool = True):
        warning = self.add_timestamp(warning)
        if log:
            logger.warning(warning)
        self.warnings.append(warning)

    def add_info(self, info: str, log: bool = True):
        info = self.add_timestamp(info)
        if log:
            logger.info(str(info))
        self.info.append(info)

    def add_error(self, error: str, log: bool = True):
        error = self.add_timestamp(error)
        if log:
            logger.error(error)
        self.errors.append(error)

    def add_response(self, response: dict, log: bool = False):
        if log:
            logger.debug(f"Response {str(response)}")
        response["timestamp"] = self.add_timestamp()
        self.responses.append(response)

    def add_config(self, i: str, config: dict):
        config["id"] = i
        self.configs.append(config)

    def add_current_config(self, config, log: bool = True):
        if log and config:
            logger.debug(f"Setting current config to {config.id}")
        self.current_config = config

    def set_status(self, status: int):
        self.status_code = status

    def add_data(self, data: dict):
        self.data.append(data)

    def formatted(self):
        returns = {}
        returns['Errors'] = self.errors
        returns['Info'] = self.info
        returns['Warnings'] = self.warnings

        if len(self.configs) > 0:
            returns['Configs'] = self.configs

        if self.current_config:
            returns['Current Config'] = self.current_config.engines

        d = [i for i in self.data if len(i) > 0]
        if len(d) > 0:
            returns['Data'] = d  # type: ignore

        if logger.log_level.debug():
            returns['_client_responses'] = self.responses # type: ignore

        return returns

    def with_status(self) -> Tuple[dict, int]:
        return (self.formatted(), self.status_code)

