from os import path
import os
import pytest  # type: ignore
from click.testing import CliRunner

from mason.cli import config, get
from mason.cli.run import run
from mason.definitions import from_root


def print_result(result):
    print(result.output)
    if result.exception:
        print(result.exception)

# @pytest.mark.skip(reason="This is not mocked, hits live endpoints")
class TestCLI:

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        examples = from_root("/examples/")
        os.environ["MASON_HOME"] = examples
        yield
        current_config = examples + "configs/CURRENT_CONFIG"
        if path.exists(current_config):
            os.remove(current_config)

    def test_play(self):
        runner = CliRunner()
        # print_result(runner.invoke(config, ["-s", "2"]))
        # print_result(runner.invoke(run, ["operator", "table", "get",  "-p", "database_name:mason-test-data,table_name:csv/test.csv,read_headers:true"], catch_exceptions=False))
        # print_result(runner.invoke(get, ["operator"], catch_exceptions=False))
        # print_result(runner.invoke(config, ["-s", "1"]))
        # print_result(runner.invoke(config, ["-s", "1"]))
        # db_name = from_root("/test/sample_data/")
        # print_result(runner.invoke(run, ["operator", "table", "summarize", "-p", f"database_name:{db_name},table_name:csv_sample.csv,read_headers:true"], catch_exceptions=False))
        # print_result(runner.invoke(config, ["-s", "5"]))
        # print_result(runner.invoke(run, ["operator", "table", "summarize", "-p", f"database_name:mason-test-data,table_name:/csv/test.csv,read_headers:true,output_path:mason-test-data/table-summary/", "-l", "trace"], catch_exceptions=False))

        print_result(runner.invoke(config, ["-s", "2"]))
        print_result(runner.invoke(run, ["operator", "database", "list"], catch_exceptions=False))
