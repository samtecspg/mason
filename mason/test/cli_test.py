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
        yield
        if path.exists(".tmp/"):
            shutil.rmtree(".tmp/")
            
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


# class TestCLI:
#     
#     @pytest.fixture(autouse=True)
#     def run_around_tests(self):
#         os.environ["MASON_HOME"] = ".tmp/"
#         yield
#         if path.exists(".tmp/"):
#             shutil.rmtree(".tmp/")
# 
#     def test_apply(self):
#         load_dotenv(from_root("/../.env"), override=True)
#         runner = CliRunner()
#         print_result(runner.invoke(apply, [from_root('/examples/')]))
#         
#         env = MasonEnvironment()
#         workflows, invalid = sequence(get_workflows(env), Workflow, InvalidObject)
#         configs, invalid = sequence(get_configs(env), Config, InvalidObject)
#         operators, invalid = sequence(get_operators(env), Operator, InvalidObject)
#         assert(len(operators) == 11)
#         assert(len(configs) == 2)
#         assert(len(workflows) == 3)


    def test_get(self):
        load_dotenv(from_root("/../.env"), override=True)
        runner = CliRunner()
        print_result(runner.invoke(apply, [from_root('/examples/')]))
        print_result(runner.invoke(get, ["all"]))

    def test_validate(self):
        load_dotenv(from_root("/../.env"), override=True)
        runner = CliRunner()
        print_result(runner.invoke(apply, [from_root('/examples/')]))
        print_result(runner.invoke(get, ["all"]))
        # print_result(runner.invoke(validate, ["operator", "table", "get", "-p", "database_name:test_db,table_name:test_table"], catch_exceptions=False))
        # print_result(runner.invoke(validate, ["operator", "table", "delete", "-p", "table_name:test,database_name:test"], catch_exceptions=False))
        # print_result(runner.invoke(run, ["operator", "table", "list", "-p", "database_name:test"], catch_exceptions=False))

