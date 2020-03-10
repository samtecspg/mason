import unittest
from unittest.mock import patch
from examples.operators.table.list import run as table_list
from examples.operators.table.test.expects import table as TableExpect # type: ignore
from configurations import Config
from parameters import Parameters
from clients.response import Response
from test.mocks import Mock
import operators as Operators
from util.environment import MasonEnvironment
from util.logger import logger

class TableTest(unittest.TestCase):

    def set_log_level(self):
        logger.set_level("info", False)
        # logger.set_level("error", False)

    def get_config(self):
        env = MasonEnvironment(operator_home="/Users/kyle/dev/mason/examples/operators")
        config = Config(env, {
            "metastore_engine": "glue",
            "clients": {
                "glue": {
                    "configuration": {
                        "aws_arn_role": "test",
                        "region": "test"
                    }
                }
            }
        })
        return config

    def test_index(self):
        self.set_log_level()
        with patch('botocore.client.BaseClient._make_api_call', new=Mock.glue):
            # Database Exists
            config = self.get_config()
            op = Operators.get_operator(config, "table", "list")
            response = Response()
            params = Parameters(parameters="database_name:crawler-poc")
            validate = params.validate(op, response)
            exists = table_list(config, params, validate)
            self.assertEqual(exists.with_status(), TableExpect.index())

            # Database DNE
            # dne = table_list(config, Parameters(parameters="database_name:bad-database"), response)
            # self.assertEqual(dne, TableExpect.index(False))

    # def test_get(self):
    #     with patch('botocore.client.BaseClient._make_api_call', new=Mock.glue):
    #         # Database and table Exist
    #         exists = Table().get("catalog_poc_data", "crawler-poc")
    #         self.assertEqual(exists, TableExpects.get(1))
    #
    #         # Database DNE
    #         dne = Table().get("catalog_poc_data", "bad-database")
    #         self.assertEqual(dne, TableExpects.get(2))
    #
    #         # Table DNE
    #         dne2 = Table().get("bad-table", "crawler-poc")
    #         self.assertEqual(dne2, TableExpects.get(3))
    #
    # def test_post(self):
    #     with patch('botocore.client.BaseClient._make_api_call', new=Mock.glue):
    #         already_exists = Table().post(
    #             {
    #                 "aws_role_arn": "arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue",
    #                 "database_name": "crawler-poc",
    #                 "s3_path": "s3://lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/",
    #                 "crawler_name": "test_crawler"
    #             }
    #         )
    #         self.assertEqual(already_exists, TableExpects.post())
    #
    #         non_existing = Table().post(
    #             {
    #                 "aws_role_arn": "arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue",
    #                 "database_name": "crawler-poc",
    #                 "s3_path": "s3://lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/",
    #                 "crawler_name": "test_crawler_new"
    #             }
    #         )
    #         self.assertEqual(non_existing, TableExpects.post(False))
    #
    #
    # def test_refresh(self):
    #     with patch('botocore.client.BaseClient._make_api_call', new=Mock.glue):
    #
    #         # valid refresh
    #         refreshing = Table().refresh("catalog_poc_data", "crawler-poc")
    #         self.assertEqual(refreshing, TableExpects.refresh(False))
    #
    #         # already refreshing
    #         already_refreshing = Table().refresh("catalog_poc_data_refreshing", "crawler-poc")
    #         self.assertEqual(already_refreshing, TableExpects.refresh())


if __name__ == '__main__':
    unittest.main()

