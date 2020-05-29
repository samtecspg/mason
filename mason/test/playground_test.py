
import os
import pytest
import dotenv
import shutil
from click.testing import CliRunner

from mason.cli import config, operator, register, workflow
from mason.definitions import from_root
from mason.test.support.testing_base import clean_string


def print_result(result):
    print()
    if result.output:
        print(result.output)
    if result.exception:
        print(result.exception)


# @pytest.mark.skip(reason="This is not mocked, hits live endpoints")
class TestPlayground:

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        os.environ["MASON_HOME"] = ".tmp/"
        yield
        shutil.rmtree(".tmp/")

    def test_unmocked(self):
        runner = CliRunner()
        print_result(runner.invoke(config, [from_root('/examples/configs/'), '-l', 'trace']))
        print_result(runner.invoke(workflow))
        print_result(runner.invoke(workflow, ['register', from_root('/examples/workflows/')]))
        print_result(runner.invoke(workflow))
        print_result(runner.invoke(workflow, ["table", "infer"]))
        print_result(runner.invoke(workflow, ["table", "infer", "-f", from_root("/examples/parameters/workfow_table_infer.yaml")]))
        print_result(runner.invoke(workflow, ["table", "infer", "-f", from_root("/examples/parameters/workflow_table_infer.yaml")]))
        print_result(runner.invoke(operator))
        print_result(runner.invoke(register, [from_root("/examples/operators/")]))
        print_result(runner.invoke(workflow, ["table", "infer", "-f", from_root("/examples/parameters/workflow_table_infer.yaml")]))
        print_result(runner.invoke(workflow, ["table", "infer", "-f", from_root("/examples/parameters/bad_workflow_table_infer.yaml")]))
        print_result(runner.invoke(workflow, ["table", "infer", "-f", from_root("/examples/parameters/workflow_table_infer.yaml"), "-d"]))
        print_result(runner.invoke(workflow, ["table", "infer", "-f", from_root("/examples/parameters/workflow_table_infer.yaml"), "-d", "-r"]))
        print_result(runner.invoke(config, ["-s", "3"]))
        print_result(runner.invoke(operator, ["table"]))
        print_result(runner.invoke(operator, ["table", "infer", "-p", "database_name:bad_database,storage_path:bogus"]))
        print_result(runner.invoke(operator, ["table", "infer", "-p", "database_name:bad_database,storage_path:spg-mason-demo/part_data_parts/", "-l", "trace"]))
        print_result(runner.invoke(operator, ["table", "infer", "-p", "database_name:crawler-poc,storage_path:spg-mason-demo/part_data_parts/", "-l", "trace"]))
        print_result(runner.invoke(config, ["-s", "1"]))
        print_result(runner.invoke(operator, ["job", "get", "-p", "job_id:a5fcd654-689d-4408-933b-395c9650b8db", "-l", "trace"]))
        print_result(runner.invoke(operator, ["table", "query", "-p", "database_name:default,query_string:SELECT * from \"part_data_parts\" limit 5", "-l", "trace"]))
        print_result(runner.invoke(operator, ["job", "get", "-p", "job_id:b2659cae-b37e-495e-bc35-6b48917f4349", "-l", "trace"]))
















