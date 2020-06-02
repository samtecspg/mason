from botocore.client import BaseClient

class LocalClient:

    def __init__(self, config: dict):
        self.threads = config.get("threads")

    def client(self) -> BaseClient:
        pass


