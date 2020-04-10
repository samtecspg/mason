
from parameters import Parameters
from operators.operator import Operator
from test.support import testing_base as base
from clients.response import Response


class TestInit:
    base.set_log_level()

    def test_good_parameter_strings(self):
        good_tests = {
            "param:value": {"param": "value"},
            "param_test-value.with.dots/and/slash:value-test_value.with.dots/and/slash": {"param_test-value.with.dots/and/slash": "value-test_value.with.dots/and/slash"},
            "param_test-value=with.equals:value-test_value=with.equals": {"param_test-value=with.equals": "value-test_value=with.equals"},
            "param1:value,param2:value": {"param1": "value", "param2": "value"},
            "param1:value,param1:value2": {"param1": "value2"},
            "testwith\,inthemiddle:result,param2:andanother\:inthemiddle": {'inthemiddle': 'result', 'param2': 'andanother'}
        }

        for param_string, result in good_tests.items():
            assert(Parameters(param_string).parsed_parameters == result)

    def test_bad_parameter_strings(self):

        bad_tests = [
            "test",
            "test,",
            "test:",
            "test:,"
        ]

        for bad in bad_tests:
            assert(Parameters(bad).parsed_parameters == {})

class TestValidation:
    def test_parameter_validation(self):

        tests = {
            "param:value": [{"param": "value"}, ["param"], 200],
            "param:value": [{}, ["other_param"], 400]
        }

        for param_string,results in tests.items():
            param = Parameters(param_string)
            op = Operator("cmd", "subcmd", "", {"required": results[1]}, [])
            validated = op.validate_params(param, Response())
            assert(validated.status_code == results[2])
            assert(param.validated_parameters == results[0])

