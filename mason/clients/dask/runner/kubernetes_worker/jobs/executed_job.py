
class ExecutedDaskJob():
    def __init__(self, message: str):
        self.message = message

class InvalidDaskJob():
    def __init__(self, reason: str):
        self.message = reason

