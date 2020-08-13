from os import path

from click.testing import CliRunner
from dotenv import load_dotenv

from mason.cli import config, register, operator
from mason.definitions import from_root
import os
import pytest  # type: ignore
import shutil


def print_result(result):
    print()
    print(result.output)
    if result.exception:
        print(result.exception)

# @pytest.mark.skip(reason="This is not mocked, hits live endpoints")
class TestCLI:

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        os.environ["MASON_HOME"] = ".tmp/"
        yield
        if path.exists(".tmp/"):
            shutil.rmtree(".tmp/")

    def test_play(self):
        load_dotenv(from_root("/../.env"), override=True)
        runner = CliRunner()
        print_result(runner.invoke(config, [from_root('/examples/configs/')]))
        print_result(runner.invoke(config, ["-s", "4"]))
        print_result(runner.invoke(register, [ from_root('/examples/') ]))
        print_result(runner.invoke(operator, ['table', 'format', '-p', 'format:xlsx,database_name:mason-sample-data,table_name:tests/in/csv/,output_path:mason-sample-data/tests/out/xlsx/'], catch_exceptions=False))
        # print_result(runner.invoke(operator, ['table', 'format', '-p', 'format:xlsx,database_name:mason-sample-data,table_name:tests/in/csv/,output_path:mason-sample-data/tests/out/xlsx/'], catch_exceptions=False))
