
class InvalidConfig():

    def __init__(self, config: dict, reason: str):
        self.config = config
        self.reason = reason
