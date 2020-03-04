
from clients.glue import GlueClient
from clients.s3 import S3Client
from typing import Optional

class Client:
    def get(self, client_name: Optional[str], config: dict):
        if client_name == "glue":
            return GlueClient(config.get("glue", {}).get("configuration", {}))
        elif client_name == "s3":
            return S3Client(config.get("s3", {}).get("configuration", {}))
        else:
            if not client_name == "None":
                print(f"Client not found {client_name}")
            return None

