from clients.response import Response

from configurations import Config
from definitions import from_root
from operators import Operator
from parameters import Parameters
from test.support.testing_base import run_tests
from util.environment import MasonEnvironment
from dotenv import load_dotenv

load_dotenv(from_root('/.env.example'))

def test_get():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid job_id
        params = Parameters(parameters=f"job_id:good_job_id")
        # TODO: consolidate these
        expect = {
            'spark': {'Data': [{'Logs': ['<LOG_DATA>']}], 'Errors': [], 'Info': [], 'Warnings': []},
            'athena': {'Data': [{'Results': [{'ResultSetMetadata': {'ColumnInfo': [{'CaseSensitive': True, 'CatalogName': 'hive', 'Label': 'widget','Name': 'widget', 'Nullable': 'UNKNOWN','Precision': 2147483647, 'Scale': 0,'SchemaName': '','TableName': '','Type': 'varchar'}]},'Rows': [{'Data': [{'VarCharValue': 'widget'}]}]}]}],'Errors': [],'Info': [], 'Warnings': []},
        }

        good = op.run(env, config, params, Response())

        assert ((expect[config.execution.client_name], 200) == good.with_status())

        # invalid job_id
        params = Parameters(parameters="job_id:bad_job_id")
        bad = op.run(env, config, params, Response())

        expect = {
            'spark': {'Errors': ['Error from server (NotFound): pods "bad_job_id-driver" not found'], 'Info': [], 'Warnings': []},
            'athena': {'Errors': ['QueryExecution bad_job_id was not found'], 'Info': [], 'Warnings': []}
        }

        assert (bad.with_status() == (expect[config.execution.client_name], 400))


    run_tests("job", "get", True, "fatal", ["config_2", "config_3"], tests)
