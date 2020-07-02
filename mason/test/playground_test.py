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

    def test_config_0(self):
        load_dotenv(from_root("/../.env"))
        runner = CliRunner()
        print_result(runner.invoke(config, [from_root('/examples/configs/')]))
        print_result(runner.invoke(config))
        print_result(runner.invoke(config, ["-s", "3"]))
        print_result(runner.invoke(register, [from_root('/examples/')]))
        print_result(runner.invoke(operator))
        # print_result(runner.invoke(workflow, ['table', 'validated_infer']))
        # print_result(runner.invoke(workflow, ['table', 'validated_infer', '-f', from_root("/examples/parameters/bad_validated_infer_1.yaml")]))
        # print_result(runner.invoke(workflow, ['table', 'validated_infer', '-f', from_root("/examples/parameters/bad_validated_infer_2.yaml")]))
        # print_result(runner.invoke(workflow, ['table', 'validated_infer', '-f', from_root("/examples/parameters/bad_validated_infer_2.yaml"), "-d"]))
        # print_result(runner.invoke(workflow, ['table', 'validated_infer', '-f', from_root("/examples/parameters/bad_validated_infer_3.yaml"), "-d", "-r"]))
        # print_result(runner.invoke(workflow, ['table', 'validated_infer', '-f', from_root("/examples/parameters/bad_validated_infer_4.yaml"), "-d", "-r"]))
        # print_result(runner.invoke(workflow, ['table', 'validated_infer', '-f', from_root("/examples/parameters/bad_validated_infer_5.yaml"), "-d", "-r"]))
        print_result(runner.invoke(workflow, ['table', 'validated_infer', '-f', from_root("/examples/parameters/validated_infer.yaml"), "-r", "-d", "-l", "info"]))

