
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
        self.reason = "Client Empty"

    def __getattr__(self, name):
        def _missing(*args, **kwargs):
            raise NotImplementedError("Method not implemented on EmptyClient")

        return _missing

    def to_dict(self):
        return {}
