from clients.response import Response

from configurations import Config
from operators.operator import Operator
from parameters import Parameters
from test.support.testing_base import run_tests
from util.environment import MasonEnvironment
import os
from dotenv import load_dotenv

def test_get():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid job_id
        job_id = "fdc01369-209c-4f22-99d0-d1e48fabcffc"
        params = Parameters(parameters=f"job_id:{job_id}")
        # expect = {'Data': [{'Logs': ['<LOG_DATA>']}], 'Errors': [], 'Info': [], 'Warnings': []}
        good = op.run(env, config, params, Response())
        # assert(good.with_status() == (expect, 200))
        print(good.formatted())

        # invalid job_id
        # params = Parameters(parameters="job_id:bad_job_id")
        # bad = op.run(env, config, params, Response())
        # expect = {'Errors': ['Error from server (NotFound): pods "bad_job_id-driver" not found'], 'Info': [], 'Warnings': []}
        # assert(bad.with_status() == (expect, 500))

    load_dotenv()
    run_tests("job", "get", True, "trace", ["config_3"], tests)
