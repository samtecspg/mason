from os import path

from click.testing import CliRunner
from dotenv import load_dotenv
from typistry.protos.invalid_object import InvalidObject

from mason.cli import apply, get 
from mason.configurations.config import Config
from mason.definitions import from_root
import os
import pytest 
import shutil

from mason.operators.operator import Operator
from mason.util.environment import MasonEnvironment
from mason.util.list import sequence
from mason.util.logger import logger
from mason.validations.validate import validate_workflows, validate_operators, validate_configs
from mason.workflows.workflow import Workflow

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
        print_result(runner.invoke(apply, [from_root('/examples/')]))
        
        env = MasonEnvironment()
        workflows, invalid = sequence(validate_workflows(env), Workflow, InvalidObject)
        configs, invalid = sequence(validate_configs(env), Config, InvalidObject)
        operators, invalid = sequence(validate_operators(env), Operator, InvalidObject)
        assert(len(workflows) == 3)
        assert(len(operators) == 11)
        assert(len(configs) == 2)
        
        logger.remove("TODO: Make this test more thorough")

    def test_get(self):
        load_dotenv(from_root("/../.env"), override=True)
        runner = CliRunner()
        print_result(runner.invoke(apply, [from_root('/examples/')]))
        # print_result(runner.invoke(get, ["operator"]))
        # print_result(runner.invoke(get, ["workflow"]))
        # print_result(runner.invoke(get, ["config"]))
        print_result(runner.invoke(get, ["all"]))
