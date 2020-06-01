
from typing import Optional

class LogLevel:
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    DEBUG = "debug"
    TRACE = "trace"
    FATAL = "fatal"
    LEVELS = [TRACE, DEBUG, INFO, WARNING, ERROR, FATAL]

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
    def fatal(self): return (self.level < 6)

class Logger:

    def __init__(self, log_level: LogLevel):
        self.log_level = log_level

    def set_level(self, log_level: Optional[str], print_message: bool = True):
        ll = LogLevel(log_level or "info")
        if ll.debug():
            print(f"Set log level to {log_level}")
        self.log_level = ll

    def log(self, message: str = ""): print(message)

    def at_level(self, level: str, message: str = ""):
        if level == "trace":
            self.trace(message)
        elif level == "debug":
            self.debug(message)
        elif level == "info":
            self.info(message)
        elif level == "warning":
            self.warning(message)
        elif level == "error":
            self.error(message)
        elif level == "fatal":
            self.fatal(message)
        else:
            pass


    def trace(self, message: str = ""):
        if self.log_level.trace():
            self.log(message)

    def debug(self, message: str = ""):
        if self.log_level.debug():
            self.log(message)

    def info(self, message: str = ""):
        if self.log_level.info():
            self.log(message)

    # special logger method thats the same as error but is specifically intended to be removed before pushing code
    def remove(self, message: str = ""):
        self.fatal()
        self.fatal("=" * 60 + "REMOVE" + "=" * 60)
        self.fatal()
        self.fatal()
        self.fatal(message)
        self.fatal()
        self.fatal()
        self.fatal("=" * 126)

    def warning(self, message: str = ""):
        if self.log_level.warning():
            self.log(message)

    def error(self, message: str = ""):
        if self.log_level.error():
            self.log(message)

    def fatal(self, message: str = ""):
        if self.log_level.fatal():
            self.log(message)


logger = Logger(LogLevel("info"))