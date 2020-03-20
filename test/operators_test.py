
from operators import operators as Operators
from test.support import testing_base as base
from clients.response import Response

class TestGetOperator:

    def test_command_exists(self):
        base.set_log_level("fatal")
        config = base.get_config("/test/support/operators", "/test/support/test_config.yaml")
        op = Operators.get_operator(config, "namespace1", "operator1")
        expects = {'cmd': 'namespace1',
                   'description': 'Test Operator',
                   'parameters': {'required': ['test_param']},
                   'subcommand': 'operator1',
                   'supported_clients': ['test_client']}
        assert(op.__dict__ == expects)

    def test_namespace_dne(self):
        base.set_log_level("fatal")
        config = base.get_config("/test/support/operators", "/test/support/test_config.yaml")
        op = Operators.get_operator(config, "namespace_dne", "operator")
        assert(op == None)

    def test_command_dne(self):
        base.set_log_level("fatal")
        config = base.get_config("/test/support/operators", "/test/support/test_config.yaml")
        op = Operators.get_operator(config, "namespace1", "operator")
        assert(op == None)


class TestListOperators:

    def test_namespace_exists(self):
        base.set_log_level("fatal")
        config = base.get_config("/test/support/operators", "/test/support/test_config.yaml")
        l = Operators.list_operators(config, "namespace1").get("namespace1")
        dicts = list(map(lambda x: x.__dict__, l))
        expects = [{'cmd': 'namespace1',
          'description': 'Test Operator',
          'parameters': {'required': ['test_param']},
          'subcommand': 'operator1',
          'supported_clients': ['test_client']},
         {'cmd': 'namespace1',
          'description': 'Test Operator',
          'parameters': {'required': ['test_param']},
          'subcommand': 'operator2',
          'supported_clients': ['unsupported_client']}]

        d = sorted(dicts, key=lambda i: i['subcommand'])
        e = sorted(expects, key=lambda e:e['subcommand'])
        assert(d == e)

    def test_namespace_dne(self):
        base.set_log_level("fatal")
        config = base.get_config("/test/support/operators", "/test/support/test_config.yaml")
        l = Operators.list_operators(config, "namespace_dne").get("namespace_dne")
        assert(l == None)

    def test_client_not_supported(self):
        response = Response()
        base.set_log_level("fatal")
        config = base.get_config("/test/support/operators", "/test/support/test_config.yaml")
        op = Operators.get_operator(config, "namespace1", "operator2")
        op, response = op.validate_configuration(config, response)
        expects = {'Errors': ["Configured clients set() not supporting operator: ['unsupported_client']"], 'Info': [], 'Warnings': []}
        assert(response.formatted() == expects)

class TestValidateOperator:

    def test_bad_operator_pat(self):
        pass

    def test_invalid_operator_definitions(self):
        pass

    def test_valid_operator_definition(self):
        pass
