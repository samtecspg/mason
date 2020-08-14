
class Schedule:

    def __init__(self, definition: str):
        self.definition = definition

class InvalidSchedule:
    
    def __init__(self, reason: str):
        self.reason = reason