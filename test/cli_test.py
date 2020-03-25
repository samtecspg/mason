
from click.testing import CliRunner
from cli import config, operator, register
from definitions import from_root
import os
import pytest #type: ignore
import shutil
import unittest
import re
from util.logger import logger

def assert_multiline(s1: str, s2: str):
    e = re.compile(r"(\s|\n|\t)")
    f1 = e.sub('', s1)
    f2 = e.sub('', s2)
    assert (f1 == f2)

def print_result(result):
    print(result.output)
    print(result.exception)

class TestCLI:

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        os.environ["MASON_HOME"] = ".tmp/"
        yield
        shutil.rmtree(".tmp/")

    def test_valid_config(self):
      runner = CliRunner()
      result = runner.invoke(config, [from_root('/examples/operators/table/test_configs/config_1.yaml'), '-l', 'info'])
      expect = """
        Set log level to info
        +-------------------------------+
        | Creating MASON_HOME at .tmp/  |
        +-------------------------------+
        +-------------------------------------------------------+
        | Creating OPERATOR_HOME at .tmp/registered_operators/  |
        +-------------------------------------------------------+
        +-----------------------------------------------+
        | Creating CONFIG_HOME at .tmp/configurations/  |
        +-----------------------------------------------+
        +--------------------+
        | Config Validation  |
        +--------------------+

        Valid Configuration. Saving config /Users/kyle/dev/mason/examples/operators/table/test_configs/config_1.yaml to .tmp/configurations/

        +----------------+
        | Configuration  |
        +----------------+
        Configuration: {'metastore': {'client_name': 'glue', 'configuration': {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}}, 'scheduler': {'client_name': 'glue', 'configuration': {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}}, 'storage': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'execution': {'client_name': '', 'configuration': {}}}
      """
      assert_multiline(expect, result.output)


    def test_bad_config(self):
        runner = CliRunner()
        self.test_valid_config()
        result = runner.invoke(config, [from_root('/examples/operators/table/test_configs/config_0.yaml'), '-l', 'info'])
        print(result.output)
        expects = """
        Set log level to info
        +--------------------+
        | Config Validation  |
        +--------------------+

        Schema error /Users/kyle/dev/mason/configurations/schema.json: 'bad' is not one of ['glue', 's3']

        Invalid Config Schema: /Users/kyle/dev/mason/examples/operators/table/test_configs/config_0.yaml
        
        """
        assert_multiline(expects, result.output)

    # def test_merge(self):
    #     runner = CliRunner()
    #     result = runner.invoke(config, [from_root('/examples/configs/config_3.yaml'), '-l', 'info'])
    #     expects = """
    #     +-------------------------------+
    #     | Creating MASON_HOME at .tmp/  |
    #     +-------------------------------+
    #     +-------------------------------------------------------+
    #     | Creating OPERATOR_HOME at .tmp/registered_operators/  |
    #     +-------------------------------------------------------+
    #     +-----------------------------------------------+
    #     | Creating CONFIG_HOME at .tmp/configurations/  |
    #     +-----------------------------------------------+
    #     +--------------------+
    #     | Config Validation  |
    #     +--------------------+
    #
    #     Valid Configuration. Saving config /Users/kyle/dev/mason/examples/configs/config_merge.yaml to .tmp/configurations/
    #
    #     +----------------+
    #     | Configuration  |
    #     +----------------+
    #     Configuration: {'metastore': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'scheduler': {'client_name': '', 'configuration': {}}, 'storage': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'execution': {'client_name': 'spark', 'configuration': {'test': 'stuff'}}}
    #
    #     """
    #     assert_multiline(expects, result.output)
    #
    #
    #     result2 = runner.invoke(register, ['/examples/operators/table/'])
    #     expects2 = """
    #     Set log level to info
    #     Registering operator(s) at /examples/operators/table/merge/ to .tmp/registered_operators/merge/
    #     """
    #     assert_multiline(expects2, result2.output)
    #
    #
    #     result3 = runner.invoke(operator, ['-l', 'info'])
    #     expects3 = """
    #         +----------------+
    #         | Configuration  |
    #         +----------------+
    #         Configuration: {'metastore': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'scheduler': {'client_name': '', 'configuration': {}}, 'storage': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'execution': {'client_name': 'spark', 'configuration': {'test': 'stuff'}}}
    #         Neither parameter string nor parameter path provided.
    #
    #         +-----------------------------------------------------------------------+
    #         | Available Operator Methods: /Users/kyle/.mason/registered_operators/  |
    #         +-----------------------------------------------------------------------+
    #
    #         namespace    command    description             parameters
    #         -----------  ---------  ----------------------  --------------------------------
    #         table        merge      Merge metastore tables  {'required': ['merge_strategy']}
    #     """
    #     assert_multiline(expects3, result3.output)
    #
    #     result4 = runner.invoke(operator, ['table', 'merge', "-p merge_strategy:test", "-l", "trace"])
    #     print_result(result4)

