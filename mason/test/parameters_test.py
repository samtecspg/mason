from typing import List, Dict

from mason.definitions import from_root
from mason.operators.operator import Operator
from mason.parameters.operator_parameters import OperatorParameters
from mason.parameters.validated_parameters import ValidatedParameters
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.test.support import testing_base as base

class TestInit:
    base.set_log_level()

    def test_good_parameter_strings(self):
        good_tests = {
            "param:value": [{"param": "value"}],
            "param_test-value.with.dots/and/slash:value-test_value.with.dots/and/slash": [{"param_test-value.with.dots/and/slash": "value-test_value.with.dots/and/slash"}],
            "param_test-value=with.equals:value-test_value=with.equals": [{"param_test-value=with.equals": "value-test_value=with.equals"}],
            "param1:value,param2:value": [{"param1": "value"}, {"param2": "value"}],
            "param1:value,param1:value2": [{"param1": "value2"}],
            "testwith\,inthemiddle:result,param2:andanother\:inthemiddle": [{'inthemiddle': 'result'}, {'param2': 'andanother\\:inthemiddle'}],
            "test with space: on both sides": [{'test with space': ' on both sides'}],
            "test with single quote:'s3://stuff here'": [{'test with single quote': 's3://stuff here'}] 
        }

        for param_string, result in good_tests.items():
            assert(OperatorParameters(param_string).to_dict() == result)

    def test_bad_parameter_strings(self):

        bad_tests = [
            "test",
            "test,",
            "test:",
            "test:,"
        ]

        for bad in bad_tests:
            assert(OperatorParameters(bad).parameters == [])

    def test_no_parameters(self):
        params = OperatorParameters()
        assert(params.invalid == [])
        assert(params.parameters == [])

    def test_from_path(self):
        params = OperatorParameters(parameter_path=from_root("/test/support/parameters/good_params.yaml"))
        assert(list(map(lambda p: p.value, params.parameters)) == ["test_value", "test_value_2"])

    def test_bad_from_path(self):
        params = OperatorParameters(parameter_path=from_root("/test/support/parameters/bad_params.yaml"))
        message = "Parameters do not conform to specified schema in parameters/schema.json.  Must be of form key:value"
        assert(params.invalid[0].reason == message)

    def test_workflow_parameters(self):
        params = WorkflowParameters(parameter_path=from_root("/test/support/parameters/good_workflow_params.yaml"))
        assert(list(map(lambda p: p.value, params.parameters[0].parameters.parameters)) == ["test_value", "test_value_2"])

    def test_bad_workflow_parameters(self):
        params = WorkflowParameters(parameter_path=from_root("/test/support/parameters/bad_workflow_params.yaml"))
        assert(f"Invalid parameters: Schema error {from_root('/parameters/workflow_schema.json')}: " in params.invalid[0].reason)

class TestValidation:
    def test_parameter_validation(self):

        tests: Dict[str, List[List[str]]] = {
            "param:value": [["param"], [], ["value"], [], ["value"]],
            "param:value,other_param:stuff": [["other_param"], ["param"], ["stuff"], ["value"], ["value", "stuff"]]
        }
        for param_string, results in tests.items():
            input_param = OperatorParameters(param_string)
            op = Operator("cmd", "subcmd", {"required": results[0], "optional": results[1]}, [])
            validated = op.parameters.validate(input_param)
            assert(isinstance(validated, ValidatedParameters))
            assert(list(map(lambda v: v.value, validated.validated_parameters)) == results[2])
            assert(list(map(lambda v: v.value, validated.optional_parameters)) == results[3])
            assert(list(map(lambda v: v.value, validated.parsed_parameters)) == results[4])


