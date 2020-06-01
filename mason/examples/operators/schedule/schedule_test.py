from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.operators.operator import Operator
from mason.parameters.input_parameters import InputParameters
from mason.test.support.testing_base import run_tests
from mason.util.environment import MasonEnvironment


def test_delete():

    def tests(env: MasonEnvironment, config: ValidConfig, op: Operator):
        # valid delete
        params = InputParameters(parameter_string=f"schedule_name:good_schedule")
        good = op.validate(config, params).run(env, Response())
        assert(good.with_status() == ({'Errors': [], 'Info': ['Schedule good_schedule successfully deleted.'], 'Warnings': []}, 200))

        # dne
        params = InputParameters(parameter_string=f"schedule_name:bad_schedule")
        good = op.validate(config, params).run(env, Response())
        assert(good.with_status() == ({'Errors': ["Crawler entry with name bad_schedule does not exist"], 'Info': [], 'Warnings': []}, 400))

    run_tests("schedule", "delete", True, "fatal", ["config_1"], tests)
