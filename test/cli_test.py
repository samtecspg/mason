
from click.testing import CliRunner
from cli import config, operator, register
from definitions import from_root
import os
import pytest #type: ignore
import shutil
import unittest

def assert_multiline(s1: str, s2: str):
    f1 = s1.replace(r"(\s|\n)", "")
    f2 = s1.replace(r"(\s|\n)", "")
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
      result = runner.invoke(config, [from_root('/examples/configs/config_example.yaml'), '-l', 'info'])
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

        Valid Configuration. Saving config examples/configs/config_example.yaml to .tmp/configurations/

        +----------------+
        | Configuration  |
        +----------------+
        Configuration: {'metastore': {'client_name': 'glue', 'configuration': {'region': 'us-east-1', 'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue'}}, 'scheduler': {'client_name': 'glue', 'configuration': {'region': 'us-east-1', 'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue'}}, 'storage': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'execution': {'client_name': '', 'configuration': {}}}

      """
      assert_multiline(expect, result.output)


    def test_bad_config(self):
        runner = CliRunner()
        result = runner.invoke(config, [from_root('/examples/configs/bad_example.yaml'), '-l', 'info'])
        expects = """
            Set log level to info
            +--------------------+
            | Config Validation  |
            +--------------------+

            Schema not found: /Users/kyle/dev/mason/.direnv/python-3.7.7/lib/python3.7/site-packages/clients/bad_client/schema.json
            Error validating client schema: bad_client

            Invalid Config Schema: examples/configs/bad_config.yaml
        """
        assert_multiline(expects, result.output)


class CLITest(unittest.TestCase):
    def test_merge(self):
        runner = CliRunner()
        result = runner.invoke(config, [from_root('/examples/configs/config_merge.yaml'), '-l', 'info'])
        expects = """
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

        Valid Configuration. Saving config /Users/kyle/dev/mason/examples/configs/config_merge.yaml to .tmp/configurations/

        +----------------+
        | Configuration  |
        +----------------+
        Configuration: {'metastore': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'scheduler': {'client_name': '', 'configuration': {}}, 'storage': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'execution': {'client_name': 'spark', 'configuration': {'test': 'stuff'}}}

        """
        assert_multiline(expects, result.output)


        result2 = runner.invoke(register, ['/examples/operators/table/'])
        expects2 = """
        Set log level to info
        Registering operator(s) at /examples/operators/table/merge/ to .tmp/registered_operators/merge/
        """
        assert_multiline(expects2, result2.output)


        result3 = runner.invoke(operator, ['-l', 'info'])
        expects3 = """
            +----------------+
            | Configuration  |
            +----------------+
            Configuration: {'metastore': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'scheduler': {'client_name': '', 'configuration': {}}, 'storage': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'execution': {'client_name': 'spark', 'configuration': {'test': 'stuff'}}}
            Neither parameter string nor parameter path provided.

            +-----------------------------------------------------------------------+
            | Available Operator Methods: /Users/kyle/.mason/registered_operators/  |
            +-----------------------------------------------------------------------+

            namespace    command    description             parameters
            -----------  ---------  ----------------------  --------------------------------
            table        merge      Merge metastore tables  {'required': ['merge_strategy']}
        """
        assert_multiline(expects3, result3.output)

        result4 = runner.invoke(operator, ['table', 'merge', "-p merge_strategy:test", "-l", "trace"])
        print_result(result4)

