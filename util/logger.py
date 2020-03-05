
from typing import Optional

class LogLevel:
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    DEBUG = "debug"
    TRACE = "trace"
    LEVELS = [TRACE, DEBUG, INFO, WARNING, ERROR]

    def __init__(self, level: Optional[str]):
        ll = level or "info"
        if ll in self.LEVELS:
            self.level = self.LEVELS.index(ll)
        else:
           raise Exception("Invalid LogLevel")

    def trace(self): self.level == True
    def debug(self): self.level > 1
    def info(self): self.level > 2
    def warning(self): self.level > 3
    def error(self): self.level > 4

class Logger:

    def __init__(self, log_level: LogLevel):
        self.log_level = log_level

    def log(self, message: str): print(message)

    def trace(self, message: str):
        if self.log_level.trace:
            self.log(message)

    def info(self, message: str):
        if self.log_level.trace:
            self.log(message)

    def debug(self, message: str):
        if self.log_level.debug:
            self.log(message)

    def warning(self, message: str):
        if  self.log_level.warning:
            self.log(message)

    def error(self, message: str):
        if self.log_level.error:
            self.log(message)

