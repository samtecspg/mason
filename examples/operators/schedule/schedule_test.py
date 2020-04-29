from clients.response import Response
from configurations import Config
from operators.operator import Operator
from parameters import Parameters
from test.support.testing_base import run_tests
from util.environment import MasonEnvironment


def test_delete():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid delete
        params = Parameters(parameters=f"schedule_name:good_schedule")
        good = op.run(env, config, params, Response())
        assert(good.with_status() == ({'Errors': [], 'Info': ['Schedule good_schedule successfully deleted.'], 'Warnings': []}, 200))

        # dne
        params = Parameters(parameters=f"schedule_name:bad_schedule")
        good = op.run(env, config, params, Response())
        assert(good.with_status() == ({'Errors': ["Crawler entry with name bad_schedule does not exist"], 'Info': [], 'Warnings': []}, 400))

    run_tests("schedule", "delete", True, "fatal", ["config_1"], tests)
