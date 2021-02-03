from os import path

from click.testing import CliRunner
from dotenv import load_dotenv

from mason.cli import apply
from mason.definitions import from_root
import os
import pytest  # type: ignore
import shutil

def print_result(result):
    print()
    print(result.output)
    if result.exception:
        print(result.exception)

class TestCLI:
    
    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        os.environ["MASON_HOME"] = ".tmp/"
        yield
        if path.exists(".tmp/"):
            shutil.rmtree(".tmp/")

    def test_apply(self):
        load_dotenv(from_root("/../.env"), override=True)
        runner = CliRunner()
        print_result(runner.invoke(apply, ["-f", from_root('/examples/')]))
        print("HERE")
