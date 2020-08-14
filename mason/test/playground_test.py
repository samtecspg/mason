from os import path

from click.testing import CliRunner
from dotenv import load_dotenv

from mason.cli import config, register, operator, workflow
from mason.definitions import from_root
import os
import pytest  # type: ignore
import shutil


def print_result(result):
    print()
    print(result.output)
    if result.exception:
        print(result.exception)


@pytest.mark.skip(reason="This is not mocked, hits live endpoints")
class TestCLI:

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        os.environ["MASON_HOME"] = ".tmp/"
        yield
        if path.exists(".tmp/"):
            shutil.rmtree(".tmp/")

    def test_play(self):
        load_dotenv(from_root("/../.env"), verbose=True, override=True)
        load_dotenv(from_root("/../.env"))
        runner = CliRunner()
        print_result(runner.invoke(config, [from_root('/examples/configs/')]))
        print_result(runner.invoke(config))
        print_result(runner.invoke(config, ["-s", "1"]))
        print_result(runner.invoke(register, [from_root('/examples/')]))
        print_result(runner.invoke(workflow, ["table", "infer", "-f", "/Users/kyle/dev/mason/mason/examples/parameters/workflow_table_infer.yaml", "-r", "-d"],catch_exceptions=False))

