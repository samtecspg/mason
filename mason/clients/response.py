from typing import List

from mason.util.logger import logger

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

    def errored(self):
        return not (len(self.errors) == 0)

    def add_warning(self, warning: str, log: bool = True):
        if log:
            logger.warning(warning)
        self.warnings.append(warning)

    def add_info(self, info: str, log: bool = True):
        if log:
            logger.info(str(info))
        self.info.append(info)

    def add_error(self, error: str, log: bool = True):
        if log:
            logger.error(error)
        self.errors.append(error)

    def add_response(self, response: dict, log: bool = False):
        if log:
            logger.debug(f"Response {str(response)}")
        self.responses.append(response)

    def add_config(self, i: str, config: dict):
        config["id"] = i
        self.configs.append(config)

    def add_current_config(self, config, log: bool = True):
        if log:
            logger.debug(f"Set current config to {config}")
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

    def with_status(self):
        return (self.formatted(), self.status_code)

