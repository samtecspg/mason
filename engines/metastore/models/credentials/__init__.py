
class MetastoreCredentials:
    def __init__(self):
        pass

    def to_dict(self): return {}

class InvalidCredentials:
    def __init__(self, reason: str):
        self.reason = reason

