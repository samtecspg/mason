from typing import List
from util.json import print_json
from util.logger import logger

class Response:
    def __init__(self):
        self.responses: List[dict] = []
        self.warnings: List[str] = []
        self.info: List[str] = []
        self.errors: List[str] = []
        self.configs = []
        self.status_code: int = 200
        self.data: dict = {}

    def errored(self):
        return not (len(self.errors) == 0)

    def add_warning(self, warning: str):
        self.warnings.append(warning)

    def add_info(self, info: str):
        self.info.append(info)

    def add_error(self, error: str):
        self.errors.append(error)

    def add_response(self, response: dict):
        self.responses.append(response)

    def add_config(self, config: dict):
        self.configs.append(config)

    def set_status(self, status: int):
        self.status_code = status

    def add_data(self, data: dict):
        self.data = data


    def formatted(self):
        returns = {}
        returns['Errors'] = self.errors
        returns['Info'] = self.info
        returns['Warnings'] = self.warnings

        if len(self.configs) > 0:
            returns['Configs'] = self.configs

        if len(self.data) > 0:
            returns['Data'] = self.data  # type: ignore

        if logger.log_level.debug():
            returns['_client_responses'] = self.responses # type: ignore

        return returns

    def with_status(self):
        return (self.formatted(), self.status_code)

