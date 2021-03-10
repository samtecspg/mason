from datetime import datetime
from typing import List, Tuple, Optional

from mason.util.logger import logger

class Response:
    def __init__(self):
        self.responses: List[dict] = []
        self.warnings: List[str] = []
        self.info: List[str] = []
        self.errors: List[str] = []
        self.status_code: int = 200
        self.data: List[dict] = []
        self.response = {}
        
    def merge(self, response: 'Response') -> 'Response':
        self.responses = self.responses + response.responses
        self.warnings = self.warnings + response.warnings
        self.info = self.info + response.info
        self.errors = self.errors + response.errors
        self.status_code = response.status_code
        self.data = self.data + response.data
        return self

    def add_timestamp(self, s: Optional[str] = None) -> str:
        if logger.log_level.level > 4:
            return s or ""
        else:
            if s:
                return f"{datetime.now()}: {s}"
            else:
                return f"{datetime.now()}"

    def errored(self):
        return not (len(self.errors) == 0)

    def add(self, key: str, value: list):
        self.response[key] = value

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

    def set_status(self, status: int):
        self.status_code = status

    def add_data(self, data: dict):
        self.data.append(data)

    def formatted(self):
        returns = self.response
        
        if len(self.errors) > 0:
            returns['Errors'] = self.errors
        if len(self.info) > 0:
            returns['Info'] = self.info
        if len(self.warnings) > 0:
            returns['Warnings'] = self.warnings

        d = [i for i in self.data if len(i) > 0]
        if len(d) > 0:
            returns['Data'] = d  # type: ignore

        if logger.log_level.debug():
            returns['_client_responses'] = self.responses # type: ignore

        return returns

    def with_status(self) -> Tuple[dict, int]:
        return (self.formatted(), self.status_code)

