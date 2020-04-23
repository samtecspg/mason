from test.support.mocks.clients.glue import GlueMock
from test.support.mocks.clients.kubernetes import KubernetesMock
from test.support.mocks.clients.s3 import S3Mock

def get_client(client: str):
    if client == "s3":
        return S3Mock()
    elif client == "glue":
        return GlueMock()
    elif client == "kubernetes":
        return KubernetesMock()
    else:
        raise Exception(f"Unmocked Client {client}")
