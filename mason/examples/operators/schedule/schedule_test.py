from mason.clients.response import Response
from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.parameters.operator_parameters import OperatorParameters
from mason.test.support.testing_base import run_tests
from mason.util.environment import MasonEnvironment

def test_delete():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid delete
        params = OperatorParameters(parameter_string=f"schedule_name:good_schedule")
        good = op.validate(config, params).run(env, Response())
        assert(good.with_status() == ({'Info': ['Schedule good_schedule successfully deleted.']}, 200))
        
        # dne
        params = OperatorParameters(parameter_string=f"schedule_name:bad_schedule")
        good = op.validate(config, params).run(env, Response())
        assert(good.with_status() == ({'Errors': ["Crawler entry with name bad_schedule does not exist"]}, 400))

    run_tests("schedule", "delete", True, "fatal", ["2"], tests)
