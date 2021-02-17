from os import path

from click.testing import CliRunner
from dotenv import load_dotenv

from mason.cli import apply, get
from mason.definitions import from_root
import os
import pytest 
import shutil

from mason.test.support.testing_base import clean_string

def print_result(result):
    print()
    print(result.output)
    if result.exception:
        print(result.exception)

def assert_equal(s1: str, s2: str):
    assert (clean_string(s1) == clean_string(s2))

class TestApply:
    pass
    
class TestGet:
    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        os.environ["MASON_HOME"] = ".tmp/"
        if path.exists(".tmp/"):
            shutil.rmtree(".tmp/")
            os.mkdir(".tmp/")
        yield
        if path.exists(".tmp/"):
            shutil.rmtree(".tmp/")
        load_dotenv(from_root('/.env.example'))
            
    def initialize(self) -> str:
        return """
            +-------------------------------+
            | Creating MASON_HOME at .tmp/  |
            +-------------------------------+
            +--------------------------------------------+
            | Creating OPERATOR_HOME at .tmp/operators/  |
            +--------------------------------------------+
            +--------------------------------------------+
            | Creating WORKFLOW_HOME at .tmp/workflows/  |
            +--------------------------------------------+
            +----------------------------------------+
            | Creating CONFIG_HOME at .tmp/configs/  |
            +----------------------------------------+
        """
    
    def test_no_arguments(self):
        runner = CliRunner()
        result = runner.invoke(get).output
        exp = "No resources. Register new resources with 'mason apply'"
        expects = self.initialize() + exp
        assert_equal(result, expects)

    def test_no_operators(self):
        runner = CliRunner()
        result = runner.invoke(get, ["operators"]).output
        exp = "No operators. Register new resources with 'mason apply'"
        expects = self.initialize() + exp
        assert_equal(result, expects)

    def test_no_workflows(self):
        runner = CliRunner()
        result = runner.invoke(get, ["workflows"]).output
        exp = "No workflows. Register new resources with 'mason apply'"
        expects = self.initialize() + exp
        assert_equal(result, expects)

    def test_no_configs(self):
        runner = CliRunner()
        result = runner.invoke(get, ["configs"]).output
        exp = "No configs. Register new resources with 'mason apply'"
        expects = self.initialize() + exp
        assert_equal(result, expects)

class TestRun:
    pass

class TestValidate:
    pass
