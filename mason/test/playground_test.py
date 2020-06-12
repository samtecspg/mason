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
    # if result.exception:
    print(result.exception)


# @pytest.mark.skip(reason="This is not mocked, hits live endpoints")
class TestCLI:

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        os.environ["MASON_HOME"] = ".tmp/"
        yield
        shutil.rmtree(".tmp/")

    def test_config_0(self):
        load_dotenv(from_root("/../.env"))
        runner = CliRunner()
        print_result(runner.invoke(config, [from_root('/examples/configs/')]))
        print_result(runner.invoke(register, [from_root('/examples/operators/')]))
        print_result(runner.invoke(workflow, ['register', from_root('/examples/workflows/')]))
        print_result(runner.invoke(operator))
        print_result(runner.invoke(workflow, ['table', 'validated_infer', '-f', from_root("/examples/parameters/validated_infer.yaml")]))
