from typing import Optional

from mason.configurations.config import Config


class InvalidConfig():

    def __init__(self, reason: str, config: Optional[Config]):
        self.reason = reason
        self.config = config
