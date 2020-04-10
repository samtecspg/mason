from clients.response import Response

from configurations import Config
from operators.operator import Operator
from parameters import Parameters
from test.support.testing_base import run_tests
from util.environment import MasonEnvironment


def test_get():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid job_id
        params = Parameters(parameters=f"job_id:good_job_id")
        expect = {'Errors': [], 'Info': [{'Errors': [], 'Info': [{'Logs': '<LOG_DATA>'}], 'Warnings': []}], 'Warnings': []}
        good = op.run(env, config, params, Response())
        assert(good.with_status() == (expect, 200))


        # invalid job_id
        params = Parameters(parameters="job_id:bad_job_id")
        bad = op.run(env, config, params, Response())
        expect = {'Errors': [], 'Info': [], 'Warnings': ['Blank response from kubectl, kubectl error handling is not good for this case, its possible the job_id is incorrect.  Check job_id']}
        assert(bad.with_status() == (expect, 200))


    run_tests("job", "get", True, tests)
