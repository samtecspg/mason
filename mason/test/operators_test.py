from os import path, mkdir
import shutil

import pytest

from mason.api.apply import apply
from mason.api.get import get
from mason.api.validate import validate
from mason.api.run import run
from mason.definitions import from_root
from mason.test.support import testing_base as base

class TestGetOperator:

    def test_command_exists(self):
        env = base.get_env("/test/support/")
        response, status = get("operator", "namespace1", "operator1", env=env) 
        expects = {'Operators': [{'command': 'operator1', 'description': 'Test Operator', 'namespace': 'namespace1', 'parameters': {'optional': [], 'required': ['test_param']}, 'supported_configurations': [{'metastore': 'test'}]}]}
        assert(response == expects)
        assert(status == 200)

    def test_command_malformed(self):
        env = base.get_env("/test/support/")
        response, status = get("operator", "namespace1", "operator3", env=env)
        assert(response['Errors'][0][0:18] == "Malformed resource")
        assert(status == 400)

    def test_namespace_dne(self):
        env = base.get_env("/test/support/")
        response, status = get("operator", "namespace_dne", "bad_command", "fatal", env)
        expects = {'Errors' : ['No operator matching namespace_dne:bad_command. Register new resources with \'mason apply\'']}
        assert(response == expects)
        assert(status == 404)

    def test_command_dne(self):
        env = base.get_env("/test/support/")
        response, status = get("operator", "namespace1", "bad_command", "fatal", env)
        expects = {'Errors' : ['No operator matching namespace1:bad_command. Register new resources with \'mason apply\'']}
        assert(response == expects)
        assert(status == 404)
        
    def test_namespace(self):
        env = base.get_env("/test/support/")
        response, status = get("operator", "namespace1", env=env, log_level="fatal")
        operators = [{'namespace': 'namespace1', 'command': 'operator1', 'description': 'Test Operator',
          'parameters': {'required': ['test_param'], 'optional': []},
          'supported_configurations': [{'metastore': 'test'}]},
         {'namespace': 'namespace1', 'command': 'operator2', 'description': 'Test Operator',
          'parameters': {'required': ['test_param'], 'optional': []},
          'supported_configurations': [{'metastore': 'test'}]}]
        assert(sorted(response['Operators'], key=lambda o: o['command']) == operators) # type: ignore
        assert(status == 200)

class TestValidateOperator:
    
    def test_valid(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        response, status = validate("operator", "namespace1", "operator1", "test_param:test", None, "3", log_level="fatal", env=env)
        expects = {'Info': ['Valid Operator: namespace1:operator1 with specified parameters.']}
        assert(response == expects)
        assert(status == 200)

    def test_invalid_parameters(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        response, status = validate("operator", "namespace1", "operator1", "asdfalskdjf", None, "3", log_level="fatal", env=env)
        expects = {'Errors': ['Invalid Resource: Invalid parameters.  Warning:  Parameter string does not conform to needed pattern: <param1>:<value1>,<param2>:<value2>, Required parameter not specified: test_param']}
        assert(response == expects)
        assert(status == 400)

    def test_bad_parameters(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        response, status = validate("operator", "namespace1", "operator1", "test_bad_param:test", None, "3", log_level="fatal", env=env)
        expects = {'Errors': ['Invalid Resource: Invalid parameters.  Required parameter not specified: test_param']}
        assert(response == expects)
        assert(status == 400)
    
    def test_invalid_config(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        response, status = validate("operator", "namespace1", "operator1", "test_param:test", None, "4", log_level="fatal", env=env)
        expects = {'Errors': ['Invalid Resource: Invalid config: Configuration 4 not supported by configured engines for operator namespace1:operator1.  Clients [] do not include supported client test for metastore. Check operator.yaml for supported engine configurations.']}
        assert(response == expects)
        assert(status == 400)

    def test_config_not_supported(self):
        pass

class TestApplyOperator:

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        tmp_folder = from_root("/.tmp/")
        if not path.exists(tmp_folder):
            mkdir(tmp_folder)
        yield
        if path.exists(tmp_folder):
            shutil.rmtree(tmp_folder)

    def test_good_operators(self):
        env = base.get_env("/.tmp/", "/test/support/validations/")
        response, status = apply(from_root("/test/support/"), env=env, log_level="fatal")
        assert(len(response["Info"]) == 20)
        assert(len(response["Errors"]) == 8)
        assert(status == 200)

        response, status = get("operator", env=env, log_level="fatal")
        assert(len(response["Operators"]) == 6)
        operators = sorted(list(map(lambda o: o["command"], response["Operators"])))
        assert(operators == ["operator1", "operator2", "operator3", "operator4", "operator5", "operator6"])
        
    def test_overwrite(self):
        pass
        

class TestRunOperator:

    def test_valid(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        response, status = run("operator", "namespace1", "operator1", "test_param:test", None, "3", log_level="fatal", env=env)
        expects = [{'Name': 'test_table', 'CreatedAt': '', 'CreatedBy': '', 'Schema': {}}] 
        assert(response['Data'] == expects)
        assert(status == 200)

