
from operators import operators as Operators
from test.support import testing_base as base
from clients.response import Response

class TestGetOperator:

    def test_command_exists(self):
        base.set_log_level("trace")
        env = base.get_env("/test/support/operators/")
        op = Operators.get_operator(env, "namespace1", "operator1")
        expects = {'cmd':'namespace1','description':'Test Operator','parameters':{'required':['test_param']},'subcommand':'operator1','supported_configurations':[{'execution':None,'metastore':'test_client','scheduler':None,'storage':None}]}
        assert(op.to_dict()==expects)

    def test_namespace_dne(self):
        base.set_log_level("fatal")
        env = base.get_env("/test/support/operators/")
        op = Operators.get_operator(env, "namespace_dne", "operator")
        assert(op == None)

    def test_command_dne(self):
        base.set_log_level("fatal")
        env = base.get_env("/test/support/operators/")
        op = Operators.get_operator(env, "namespace1", "operator")
        assert(op == None)


class TestListOperators:

    def test_namespace_exists(self):
        base.set_log_level("fatal")
        env = base.get_env("/test/support/operators/")
        l = Operators.list_operators(env, "namespace1").get("namespace1")
        dicts = list(map(lambda x: x.to_dict(), l))
        expects = [{'cmd': 'namespace1',
          'description': 'Test Operator',
          'parameters': {'required': ['test_param']},
          'subcommand': 'operator1',
          'supported_configurations': [{'execution': None,
                                        'metastore': 'test_client',
                                        'scheduler': None,
                                        'storage': None}]},
         {'cmd': 'namespace1',
          'description': 'Test Operator',
          'parameters': {'required': ['test_param']},
          'subcommand': 'operator2',
          'supported_configurations': [{'execution': None,
                                        'metastore': 'unsupported_client',
                                        'scheduler': None,
                                        'storage': None}]}]

        d = sorted(dicts, key=lambda i: i['subcommand'])
        e = sorted(expects, key=lambda e:e['subcommand'])
        assert(d == e)

    def test_namespace_dne(self):
        base.set_log_level("fatal")
        env = base.get_env("/test/support/operators/")
        l = Operators.list_operators(env, "namespace_dne").get("namespace_dne")
        assert(l == None)

    def test_client_not_supported(self):
        response = Response()
        base.set_log_level("fatal")
        env = base.get_env("/test/support/operators/")
        config = base.get_configs(env)[0]
        op = Operators.get_operator(env, "namespace1", "operator2")
        response = op.validate_configuration(config, response)
        expects = {'Errors': ['Configuration not supported by configured engines.  Check operator.yaml for supported engine configurations.'], 'Info': [], 'Warnings': []}
        assert(response.formatted() == expects)

class TestValidateOperator:

    def test_bad_operator_pat(self):
        pass

    def test_invalid_operator_definitions(self):
        pass

    def test_valid_operator_definition(self):
        pass
