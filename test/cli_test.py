
from click.testing import CliRunner
from cli import config
from definitions import from_root
import os
from util.logger import logger
import pytest #type: ignore
import shutil

def assert_multiline(s1: str, s2: str):
    f1 = s1.replace("(\s|\n)", "")
    f2 = s1.replace("(\s|\n)", "")
    assert (f1 == f2)

class TestCLI:

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        os.environ["MASON_HOME"] = ".tmp/"
        yield
        shutil.rmtree(".tmp")

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


