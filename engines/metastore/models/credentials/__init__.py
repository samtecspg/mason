
class MetastoreCredentials:
    def __init__(self):
        self.type = ""
        self.access_key = None
        self.secret_key = None

    def empty(self):
        ((self.access_key or "") == "") or ((self.secret_key or "") == "")

