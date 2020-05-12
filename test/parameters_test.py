
from parameters import Parameters, InputParameters
from operators.operator import Operator
from test.support import testing_base as base
from clients.response import Response


class TestInit:
    base.set_log_level()

    def test_good_parameter_strings(self):
        good_tests = {
            "param:value": [{"param": "value"}],
            "param_test-value.with.dots/and/slash:value-test_value.with.dots/and/slash": [{"param_test-value.with.dots/and/slash": "value-test_value.with.dots/and/slash"}],
            "param_test-value=with.equals:value-test_value=with.equals": [{"param_test-value=with.equals": "value-test_value=with.equals"}],
            "param1:value,param2:value": [{"param1": "value"}, {"param2": "value"}],
            "param1:value,param1:value2": [{"param1": "value2"}],
            "testwith\,inthemiddle:result,param2:andanother\:inthemiddle": [{'inthemiddle': 'result'}, {'param2': 'andanother\\'}],
            "test with space: on both sides": [{'test with space': ' on both sides'}]
        }

        for param_string, result in good_tests.items():
            assert(InputParameters(param_string).to_dict() == result)

    def test_bad_parameter_strings(self):

        bad_tests = [
            "test",
            "test,",
            "test:",
            "test:,"
        ]

        for bad in bad_tests:
            assert(InputParameters(bad).parameters == [])

    def test_no_parameters(self):
        params = InputParameters()



class TestValidation:
    def test_parameter_validation(self):

        tests = {
            "param:value": [{"param": "value"}, ["param"], 200],
            "param:value": [{}, ["other_param"], 400]
        }

        for param_string,results in tests.items():
            input_param = InputParameters(param_string)
            op = Operator("cmd", "subcmd", "", {"required": results[1]}, [])
            validated = op.parameters.validate(input_param)
            # assert(validated.status_code == results[2])
            # assert(param.validated_parameters == results[0])

