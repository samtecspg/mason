
from parameters import Parameters, ParameterException
from configurations.valid_operator import ValidOperator
import pytest # type: ignore
from test.support import testing_base as base

class TestInit:
    base.set_log_level()

    def test_parameter_strings(self):
        good_tests = {
            "param:value": {"param": "value"},
            "param_test-value.with.dots/and/slash:value-test_value.with.dots/and/slash": {"param_test-value.with.dots/and/slash": "value-test_value.with.dots/and/slash"},
            "param1:value,param2:value": {"param1": "value", "param2": "value"},
            "param1:value,param1:value2": {"param1": "value2"},
            "testwith\,inthemiddle:result,param2:andanother\:inthemiddle": {'inthemiddle': 'result', 'param2': 'andanother'}
        }

        bad_tests = [
            "test",
            "test,",
            "test:",
            "test:,",
            "test:res"
        ]

        for param_string, result in good_tests.items():
            assert(Parameters(param_string).parsed_parameters == result)

        with pytest.raises(ParameterException):
            for bad in bad_tests:
                Parameters(bad)

    def test_parameter_validation(self):

        tests = {
            "param:value": [{"param": "value"}, ["param"], 200],
            "param:value": [{}, ["other_param"], 400]
        }

        for param_string,results in tests.items():
            param = Parameters(param_string)
            op = ValidOperator("cmd", "subcmd", results[1], [])
            validated = param.validate(op)
            assert(validated.status_code == results[2])
            assert(param.validated_parameters == results[0])

