
class DDLStatement:

    def __init__(self, statement: str):
        self.statement = statement

class InvalidDDLStatement:

    def __init__(self, reason: str):
        self.reason = reason

