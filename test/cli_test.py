
from click.testing import CliRunner
from cli import config, operator, register
from definitions import from_root
import os
import pytest #type: ignore
import shutil
from test.support.testing_base import assert_multiline
from util.uuid import uuid_regex
import time

#  TODO: Figure out how to remove references to /Users/kyle/dev from these tests for any user, remove basename from output

def print_result(result):
    print()
    print(result.output)
    print(result.exception)

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

        Schema error /Users/kyle/dev/mason/configurations/schema.json: 'bad' is not one of ['glue', 's3']

        No bad client configuration for specified metastore_engine

        No s3 client configuration for specified storage_engine

        Invalid config schema: {'metastore_engine': 'bad', 'storage_engine': 's3'}        
        
        Config 0 not found
        """
        print_result(result)
        # assert_multiline(expects, result.output)

    def test_config_1(self):
      runner = CliRunner()
      result1 = runner.invoke(config, [from_root('/examples/operators/table/test_configs/config_1.yaml'), '-l', 'info'])
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
        
        Set log level to info

        Valid Configuration. Saving config /Users/kyle/dev/mason/examples/operators/table/test_configs/config_1.yaml to .tmp/configurations/

        +------------------------------+
        | Setting current config to 0  |
        +------------------------------+
        +-----------------+
        | Configurations  |
        +-----------------+
        Config ID    Engine     Client    Configuration
        -----------  ---------  --------  --------------------------------------------------------------------------------------------------------------------------
        *  0         metastore  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     scheduler  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     storage    s3        {'region': 'us-east-1'}

        * = Current Configuration      
      """
      assert_multiline(expect1, result1.output)

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
        
        Valid Configuration: {'metastore': {'client_name': 'glue', 'configuration': {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}}, 'scheduler': {'client_name': 'glue', 'configuration': {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}}, 'storage': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'execution': {'client_name': '', 'configuration': {}}}

        Valid Configuration. Saving config /Users/kyle/dev/mason/examples/operators/table/test_configs/config_1.yaml to .tmp/configurations/


        Schema error /Users/kyle/dev/mason/configurations/schema.json: 'bad' is not one of ['glue', 's3']

        No bad client configuration for specified metastore_engine

        No s3 client configuration for specified storage_engine

        Invalid config schema: {'metastore_engine': 'bad', 'storage_engine': 's3'}

        Valid Configuration: {'metastore': {'client_name': 's3', 'configuration': {'region': 'us-east-1', 'access_key': '***REMOVED***', 'secret_key': '***REMOVED***'}}, 'scheduler': {'client_name': '', 'configuration': {}}, 'storage': {'client_name': '', 'configuration': {}}, 'execution': {'client_name': 'spark', 'configuration': {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}}}

        Valid Configuration. Saving config /Users/kyle/dev/mason/examples/operators/table/test_configs/config_2.yaml to .tmp/configurations/

        Reading configurations at .tmp/configurations/
        Valid Configuration: {'metastore': {'client_name': 'glue', 'configuration': {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}}, 'scheduler': {'client_name': 'glue', 'configuration': {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}}, 'storage': {'client_name': 's3', 'configuration': {'region': 'us-east-1'}}, 'execution': {'client_name': '', 'configuration': {}}}
        Valid Configuration: {'metastore': {'client_name': 's3', 'configuration': {'region': 'us-east-1', 'access_key': '***REMOVED***', 'secret_key': '***REMOVED***'}}, 'scheduler': {'client_name': '', 'configuration': {}}, 'storage': {'client_name': '', 'configuration': {}}, 'execution': {'client_name': 'spark', 'configuration': {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}}}
        +------------------------------+
        | Setting current config to 0  |
        +------------------------------+
        +-----------------+
        | Configurations  |
        +-----------------+
        Config ID    Engine     Client    Configuration
        -----------  ---------  --------  --------------------------------------------------------------------------------------------------------------------------
        *  0         metastore  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     scheduler  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     storage    s3        {'region': 'us-east-1'}
        .  1         metastore  s3        {'region': 'us-east-1', 'access_key': '***REMOVED***', 'secret_key': '***REMOVED***'}
                     execution  spark     {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}
        * = Current Configuration
        """
        assert_multiline(expect1, result1.output)
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
        .  1         metastore  s3        {'region': 'us-east-1', 'access_key': '***REMOVED***', 'secret_key': '***REMOVED***'}
                     execution  spark     {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}

        * = Current Configuration
        """
        assert_multiline(expects2, result2.output)
        result3 = runner.invoke(config, ['-s', '1', '-l', 'info'])
        expects3 = """
        Set log level to info
        +------------------------------+
        | Setting current config to 1  |
        +------------------------------+
        +-----------------+
        | Configurations  |
        +-----------------+
        Config ID    Engine     Client    Configuration
        -----------  ---------  --------  --------------------------------------------------------------------------------------------------------------------------
        .  0         metastore  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     scheduler  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     storage    s3        {'region': 'us-east-1'}
        *  1         metastore  s3        {'region': 'us-east-1', 'access_key': '***REMOVED***', 'secret_key': '***REMOVED***'}
                     execution  spark     {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}

        * = Current Configuration
        """
        assert_multiline(expects3, result3.output)

    @pytest.mark.skip(reason="This is not mocked, hits live endpoints")
    def test_config_2(self):
        runner = CliRunner()
        result1 = runner.invoke(config, [from_root('/examples/configs/'), '-l', 'info'])
        expects1 = """
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

        Valid Configuration. Saving config /Users/kyle/dev/mason/examples/configs/config_1.yaml to .tmp/configurations/
        Valid Configuration. Saving config /Users/kyle/dev/mason/examples/configs/config_2.yaml to .tmp/configurations/

        +------------------------------+
        | Setting current config to 0  |
        +------------------------------+
        +-----------------+
        | Configurations  |
        +-----------------+
        Config ID    Engine     Client    Configuration
        -----------  ---------  --------  --------------------------------------------------------------------------------------------------------------------------
        *  0         metastore  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     scheduler  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     storage    s3        {'region': 'us-east-1'}
        .  1         metastore  s3        {'region': 'us-east-1', 'access_key': '***REMOVED***', 'secret_key': '***REMOVED***'}
                     execution  spark     {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}

        * = Current Configuration
        """
        assert_multiline(result1.output, expects1)

        result2 = runner.invoke(register, [from_root('/examples/operators/table/')])
        expects2 = """
        Set log level to info
        Valid Operator Definition /Users/kyle/dev/mason/examples/operators/table/merge/operator.yaml
        Valid Operator Definition /Users/kyle/dev/mason/examples/operators/table/refresh/operator.yaml
        Valid Operator Definition /Users/kyle/dev/mason/examples/operators/table/get/operator.yaml
        Valid Operator Definition /Users/kyle/dev/mason/examples/operators/table/list/operator.yaml
        Valid Operator Definition /Users/kyle/dev/mason/examples/operators/table/infer/operator.yaml
        Registering operators at /Users/kyle/dev/mason/examples/operators/table/ to .tmp/registered_operators/table/
        """
        assert_multiline(result2.output, expects2)

        result25 = runner.invoke(register, [from_root('/examples/operators/job/')])
        expects25 = """
        Set log level to info
        Valid Operator Definition /Users/kyle/dev/mason/examples/operators/job/get/operator.yaml
        Registering operators at /Users/kyle/dev/mason/examples/operators/job/ to .tmp/registered_operators/job/
        """
        assert_multiline(expects25, result25.output)

        result3 = runner.invoke(operator, [])
        expects3 = """
        Neither parameter string nor parameter path provided.

        +---------------------------------------------------------+
        | Available Operator Methods: .tmp/registered_operators/  |
        +---------------------------------------------------------+

        namespace    command    description                                                                               parameters
        -----------  ---------  ----------------------------------------------------------------------------------------  ----------------------------------------------------------------------------------------------
        table        merge      Merge metastore tables                                                                    {'required': ['output_path', 'input_path'], 'optional': ['extract_paths', 'repartition_keys']}
        table        refresh    Refresh metastore tables                                                                  {'required': ['database_name', 'table_name']}
        table        get        Get metastore table contents                                                              {'required': ['database_name', 'table_name']}
        table        list       Get metastore tables                                                                      {'required': ['database_name']}
        table        infer      Registers a schedule for infering the table then does a one time trigger of the refresh.  {'required': ['database_name', 'storage_path', 'schedule_name']}
        job          get        Get Execution Job Status                                                                  {'required': ['job_id']}
        """
        # assert_multiline(result3.output, expects3)

        result4 = runner.invoke(config, ["-s", "1"])
        expects4 = """
        Set log level to info
        +------------------------------+
        | Setting current config to 1  |
        +------------------------------+
        +-----------------+
        | Configurations  |
        +-----------------+
        Config ID    Engine     Client    Configuration
        -----------  ---------  --------  --------------------------------------------------------------------------------------------------------------------------
        .  0         metastore  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     scheduler  glue      {'aws_role_arn': 'arn:aws:iam::062325279035:role/service-role/AWSGlueServiceRole-anduin-data-glue', 'region': 'us-east-1'}
                     storage    s3        {'region': 'us-east-1'}
        *  1         metastore  s3        {'region': 'us-east-1', 'access_key': '***REMOVED***', 'secret_key': '***REMOVED***'}
                     execution  spark     {'runner': {'spark_version': '2.4.5', 'type': 'kubernetes-operator'}}

        * = Current Configuration 
        """
        assert_multiline(expects4, result4.output)

        result5 = runner.invoke(operator, ["table", "get", "-l", "trace", "-p", "database_name:spg-mason-demo,table_name:part_data/"])

        # result6 = runner.invoke(operator, ["table", "merge", "-l", "trace", "-p", "input_path:spg-mason-demo/part_data/,output_path:spg-mason-demo/merged/"])
        # print_result(result6)

        # uuid = uuid_regex().findall(result6.output)[0]
        # uuid = "f412dd60-b6f7-449e-9ac4-2f592f74fd1f"
        # print(uuid)

        # job_id = f"mason-spark-merge-{uuid}"

        # time.sleep(10)

        # # result7 = runner.invoke(operator, ["job", "get", "-l", "trace", "-p", f"job_id:{job_id}"], catch_exceptions=False)
        # print_result(result7)

        # result8 = runner.invoke(operator, ["table", "get", "-l", "trace", "-p", f"database_name:spg-mason-demo,table_name:merged"])
        # print_result(result8)


