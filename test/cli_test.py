
from click.testing import CliRunner
from cli import config, operator, register
from definitions import from_root
import os
import pytest #type: ignore
import dotenv #type: ignore
import shutil
from test.support.testing_base import clean_string



def print_result(result):
    print()
    print(result.output)
    print(result.exception)


@pytest.mark.skip(reason="This is not mocked, hits live endpoints")
class TestCLI:

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        os.environ["MASON_HOME"] = ".tmp/"
        yield
        shutil.rmtree(".tmp/")


    def test_config_0(self):
        runner = CliRunner()

        result = runner.invoke(config, [from_root('/examples/operators/table/test_configs/config_0.yaml'), '-l', 'info'])
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
        Set log level to info

        Schema error """ + from_root("/configurations/schema.json") + """: 'bad' is not one of ['glue', 's3']

        No bad client configuration for specified metastore_engine

        No s3 client configuration for specified storage_engine

        Invalid config schema: {'metastore_engine': 'bad', 'storage_engine': 's3'}
        
        Config 0 not found
        """
        assert clean_string(expects) == clean_string(result.output)

    def test_config_1(self):
      dotenv.load_dotenv()
      runner = CliRunner()
      result1 = runner.invoke(config, [from_root('/examples/operators/table/test_configs/config_1.yaml'), '-l', 'info'])
      result2 = runner.invoke(register, [from_root('/examples/operators/table/'), '-l', 'info'])
      result3 = runner.invoke(operator, ["table", "list", "-p", "database_name:crawler-poc"])
      print_result(result3)

    def test_set_current_config(self):
        runner = CliRunner()
        result1 = runner.invoke(config, [from_root('/examples/operators/table/test_configs/'), '-l', 'trace'])

        expect1 = """
        +-------------------------------+
        | Creating MASON_HOME at .tmp/  |
        +-------------------------------+
        +-------------------------------------------------------+
        | Creating OPERATOR_HOME at .tmp/registered_operators/  |
        +-------------------------------------------------------+
        +-----------------------------------------------+
        | Creating CONFIG_HOME at .tmp/configurations/  |
        +-----------------------------------------------+
        Set log level to trace


        Schema error """ + from_root("/configurations/schema.json") + """: 'bad' is not one of ['glue', 's3']

        No bad client configuration for specified metastore_engine

        No s3 client configuration for specified storage_engine

        Invalid config schema: {'metastore_engine': 'bad', 'storage_engine': 's3'}

        Valid Configuration: {'metastore': {'client_name': 'glue', 'configuration': {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}}, 'scheduler': {'client_name': 'glue', 'configuration': {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}}, 'storage': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'execution': {'client_name': '', 'configuration': {}}}
        Valid Configuration. Saving config """ + from_root("/examples/operators/table/test_configs/config_1.yaml") + """ to .tmp/configurations/
        Valid Configuration: {'metastore': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'scheduler': {'client_name': '', 'configuration': {}}, 'storage': {'client_name': '', 'configuration': {}}, 'execution': {'client_name': 'spark', 'configuration': {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}}}
        Valid Configuration. Saving config """ + from_root("/examples/operators/table/test_configs/config_2.yaml") + """ to .tmp/configurations/

        Reading configurations at .tmp/configurations/
        Valid Configuration: {'metastore': {'client_name': 'glue', 'configuration': {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}}, 'scheduler': {'client_name': 'glue', 'configuration': {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}}, 'storage': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'execution': {'client_name': '', 'configuration': {}}}
        Valid Configuration: {'metastore': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'scheduler': {'client_name': '', 'configuration': {}}, 'storage': {'client_name': '', 'configuration': {}}, 'execution': {'client_name': 'spark', 'configuration': {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}}}
        Setting current config to 0
        +-----------------+
        | Configurations  |
        +-----------------+
        Config ID    Engine     Client    Configuration
        -----------  ---------  --------  --------------------------------------------------------------------------------------------------------------------------
        *  0         metastore  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     scheduler  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     storage    s3        {'region': 'us-east-1'}
        .  1         metastore  s3        {'region': 'us-east-1'}
                     execution  spark     {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}

        * = Current Configuration
        """
        assert clean_string(expect1) == clean_string(result1.output)

        result2 = runner.invoke(config, ['-l', 'info'])
        expects2 = """
        Set log level to info
        +-----------------+
        | Configurations  |
        +-----------------+
        Config ID    Engine     Client    Configuration
        -----------  ---------  --------  --------------------------------------------------------------------------------------------------------------------------
        *  0         metastore  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     scheduler  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     storage    s3        {'region': 'us-east-1'}
        .  1         metastore  s3        {'region': 'us-east-1'}
                     execution  spark     {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}

        * = Current Configuration
        """
        assert clean_string(expects2) == clean_string(result2.output)
        result3 = runner.invoke(config, ['-s', '1', '-l', 'info'])
        expects3 = """
        Set log level to info
        Setting current config to 1
        +-----------------+
        | Configurations  |
        +-----------------+
        Config ID    Engine     Client    Configuration
        -----------  ---------  --------  --------------------------------------------------------------------------------------------------------------------------
        .  0         metastore  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     scheduler  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     storage    s3        {'region': 'us-east-1'}
        *  1         metastore  s3        {'region': 'us-east-1'}
                     execution  spark     {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}

        * = Current Configuration
        """
        assert clean_string(expects3) == clean_string(result3.output)

    def test_config_1_unmocked(self):
        runner = CliRunner()
        result1 = runner.invoke(config, [from_root('/examples/operators/table/test_configs/config_1.yaml'), '-l', 'info'])
        result2 = runner.invoke(register, [from_root('/examples/operators/table/')])
        result3 = runner.invoke(operator, ["table", "list", "-p", "database_name:crawler-poc", "-l", "trace"])
        result4 = runner.invoke(operator, ["table", "get", "-p", "database_name:crawler-poc,table_name:catalog_poc_data", "-l", "trace"])
        result5 = runner.invoke(operator, ["table", "refresh", "-p", "database_name:crawler-poc,table_name:catalog_poc_data", "-l", "trace"])

    def test_config_2_csv_unmocked(self):
        runner = CliRunner()
        result1 = runner.invoke(config, [from_root('/examples/configs/'), '-l', 'info'])
        result2 = runner.invoke(register, [from_root('/examples/operators/table/')])
        result25 = runner.invoke(register, [from_root('/examples/operators/job/')])
        result4 = runner.invoke(config, ["-s", "1"])
        result5 = runner.invoke(operator, ["table", "get", "-l", "error", "-p", "database_name:spg-mason-demo,table_name:conflicting-parquet/"])
        print_result(result5)


    def test_config_2_unmocked(self):
        runner = CliRunner()
        result1 = runner.invoke(config, [from_root('/examples/configs/'), '-l', 'info'])
        print_result(result1)
        result2 = runner.invoke(register, [from_root('/examples/operators/table/')])
        print_result(result2)
        result25 = runner.invoke(register, [from_root('/examples/operators/job/')])
        print_result(result25)
        result3 = runner.invoke(operator, [])
        print_result(result3)
        result4 = runner.invoke(config, ["-s", "1"])
        print_result(result4)
        result5 = runner.invoke(operator, ["job", "get", "-l", "trace", "-p", f"job_id:{job_id}"], catch_exceptions=False)
        print_result(result5)


