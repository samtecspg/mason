from clients.response import Response

from configurations import Config
from operators.operator import Operator
from parameters import Parameters
from test.support.testing_base import run_tests
from util.environment import MasonEnvironment
import os
from dotenv import load_dotenv #type: ignore

def test_get():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid job_id
        params = Parameters(parameters=f"job_id:good_job_id")
        # TODO: consolidate these
        expect = {
            'spark': {'Data': [{'Logs': ['<LOG_DATA>']}], 'Errors': [], 'Info': [], 'Warnings': []},
            'athena': {'Errors': [], 'Info': [], 'Warnings': [], 'Data': [{'Results': [{'Rows': [{'Data': [{'VarCharValue': 'index'}]}], 'ResultSetMetadata': {'ColumnInfo': [{'CatalogName': 'hive', 'SchemaName': '', 'TableName': '', 'Name': 'index', 'Label': 'index','Type': 'bigint', 'Precision': 19, 'Scale': 0, 'Nullable': 'UNKNOWN', 'CaseSensitive': False}]}}]}]}
        }

        good = op.run(env, config, params, Response())

        assert (good.with_status() == (expect[config.execution.client_name], 200))

        # invalid job_id
        params = Parameters(parameters="job_id:bad_job_id")
        bad = op.run(env, config, params, Response())

        expect = {
            'spark': {'Errors': ['Error from server (NotFound): pods "bad_job_id-driver" not found'], 'Info': [], 'Warnings': []},
            'athena': {'Errors': ['QueryExecution bad_job_id was not found'], 'Info': [], 'Warnings': []}
        }

        assert (bad.with_status() == (expect[config.execution.client_name], 400))


    load_dotenv()
    run_tests("job", "get", True, "fatal", ["config_2", "config_3"], tests)
