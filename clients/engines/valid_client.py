
class ValidClient:

    def __init__(self, client_name: str, config: dict):
        self.client_name = client_name
        self.config = config

    def to_dict(self):
        return {
            "client_name": self.client_name,
            "configuration": self.config
        }


class EmptyClient():
    def __init__(self):
        self.client_name = ""

    def to_dict(self):
        return {}
