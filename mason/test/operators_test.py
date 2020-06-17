import shutil
import os

from mason.clients.response import Response
from mason.configurations.invalid_config import InvalidConfig
from mason.definitions import from_root
from mason.operators.operator import emptyOperator
from mason.test.support import testing_base as base
from mason.util.logger import logger
from mason.util.list import flatten_array
from mason.operators import operators, namespaces


class TestRegisterOperator:

    def test_register_to(self):
        base.set_log_level("fatal")
        mason_home = from_root("/.tmp/")
        if os.path.exists(mason_home):
            shutil.rmtree(mason_home)

        env = base.get_env("/.tmp/operators/")

        ops, errors = operators.list_operators(from_root("/test/support/operators"))
        for operator in ops:
            operator.register_to(env.operator_home)

        ns, invalid = operators.list_namespaces(env.operator_home)

        result = sorted(list(map(lambda n: n.to_dict_brief(), ns)), key=lambda s: list(s.keys())[0])
        expect = [{'namespace1': ['operator1', 'operator2']}, {'namespace2': ['operator3', 'operator4', 'operator5', 'operator6']}]

        assert(result == expect)

        if os.path.exists(mason_home):
            shutil.rmtree(mason_home)

class TestGetOperator:

    def test_command_exists(self):
        base.set_log_level("trace")
        env = base.get_env("/test/support/operators/")
        op = (operators.get_operator(env.operator_home, "namespace1", "operator1") or emptyOperator())
        expects = {'command': 'operator1', 'description': 'Test Operator', 'namespace': 'namespace1', 'parameters': {'optional': [], 'required': ['test_param']}, 'supported_configurations': [{'execution': None, 'metastore': 'test', 'scheduler': None, 'storage': None}]}
        assert(op.to_dict()==expects)

    def test_namespace_dne(self):
        base.set_log_level("fatal")
        env = base.get_env("/test/support/operators/")
        op = operators.get_operator(env.operator_home, "namespace_dne", "operator")
        assert(op == None)

    def test_command_dne(self):
        base.set_log_level("fatal")
        env = base.get_env("/test/support/operators/")
        op = operators.get_operator(env.operator_home, "namespace1", "operator")
        assert(op == None)


class TestListOperators:

    def test_namespace_exists(self):
        base.set_log_level("fatal")
        env = base.get_env("/test/support/operators/")
        l = operators.list_namespaces(env.operator_home, "namespace1")[0]
        dicts = flatten_array(list(map(lambda x: x.to_dict(), l)))
        expects = [{'namespace': 'namespace1',
          'description': 'Test Operator',
          'parameters': {'required': ['test_param'], 'optional': []},
          'command': 'operator1',
          'supported_configurations': [{'execution': None,
                                        'metastore': 'test',
                                        'scheduler': None,
                                        'storage': None}]},
         {'namespace': 'namespace1',
          'description': 'Test Operator',
          'parameters': {'required': ['test_param'], 'optional': []},
          'command': 'operator2',
          'supported_configurations': [{'execution': None,
                                        'metastore': 'test',
                                        'scheduler': None,
                                        'storage': None}]}]

        d = sorted(dicts, key=lambda i: i['command'])
        e = sorted(expects, key=lambda e: e['command'])
        assert(d == e)

    def test_namespace_dne(self):
        base.set_log_level("fatal")
        env = base.get_env("/test/support/operators/")
        l = namespaces.get(operators.list_namespaces(env.operator_home, "namespace_dne")[0], "namespace_dne", "cmd")

        assert(l == None)

    def test_client_not_supported(self):
        response = Response()
        base.set_log_level("fatal")
        env = base.get_env("/test/support/operators/")
        config = base.get_configs(env)[0]
        op = operators.get_operator(env.operator_home, "namespace1", "operator2") or emptyOperator()
        if op:
            valid = op.validate_config(config)
            if isinstance(valid, InvalidConfig):
                expects = 'Configuration not supported by configured engines.  Check operator.yaml for supported engine configurations.'
                assert(valid.reason == expects)
            else:
                raise Exception("BadTest")

class TestValidateOperator:

    def test_bad_operator_pat(self):
        pass

    def test_invalid_operator_definitions(self):
        pass

    def test_valid_operator_definition(self):
        pass
