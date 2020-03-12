
from typing import Optional

class LogLevel:
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    DEBUG = "debug"
    TRACE = "trace"
    LEVELS = [TRACE, DEBUG, INFO, WARNING, ERROR]

    def __init__(self, level: str):
        if level in self.LEVELS:
            self.level = self.LEVELS.index(level)
        else:
           raise Exception("Invalid LogLevel")

    def trace(self): return (self.level < 1)
    def debug(self): return (self.level < 2)
    def info(self): return (self.level < 3)
    def warning(self): return (self.level < 4)
    def error(self): return (self.level < 5)

class Logger:

    def __init__(self, log_level: LogLevel):
        self.log_level = log_level

    def set_level(self, log_level: Optional[str], print_message: bool = True):
        ll = log_level or "info"
        if print_message:
            print(f"Set log level to {ll}")
        self.log_level = LogLevel(ll)

    def log(self, message: str = ""): print(message)

    def trace(self, message: str = ""):
        if self.log_level.trace():
            self.log(message)

    def debug(self, message: str = ""):
        if self.log_level.debug():
            self.log(message)

    def info(self, message: str = ""):
        if self.log_level.info():
            self.log(message)

    # special logger method thats the same as info but is specifically intended to be removed before pushing code
    def remove(self, message: str = ""):
        self.error()
        self.error("=" * 60 + "REMOVE" + "=" * 60)
        self.error()
        self.error()
        self.error(message)
        self.error()
        self.error()
        self.error("=" * 126)

    def warning(self, message: str = ""):
        if self.log_level.warning():
            self.log(message)

    def error(self, message: str = ""):
        if self.log_level.error():
            self.log(message)


logger = Logger(LogLevel("info"))