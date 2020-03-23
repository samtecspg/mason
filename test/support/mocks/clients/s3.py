
from test.support.mocks.mock_base import MockBase
from clients.s3.metastore import S3MetastoreClient

class S3Mock(MockBase, S3MetastoreClient):

    def method(self, operation_name, kwarg):
        raise Exception(f"Unmocked S3 API endpoint: {operation_name}")



