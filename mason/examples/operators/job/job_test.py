from mason.clients.response import Response
from mason.configurations.config import Config
from mason.definitions import from_root
from mason.operators.operator import Operator
from mason.parameters.operator_parameters import OperatorParameters
from mason.test.support.testing_base import run_tests
from mason.util.environment import MasonEnvironment
from dotenv import load_dotenv

load_dotenv(from_root('/.env.example'))

def test_get():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid job_id
        params = OperatorParameters(parameter_string=f"job_id:good_job_id")
        # TODO: consolidate these
        expect = {
            'spark': {'Data': [{'Logs': ['<LOG_DATA>']}]},
            'athena': {'Data': [{'ResultSetMetadata': {'ColumnInfo': [{'CaseSensitive': True, 'CatalogName': 'hive', 'Label': 'widget','Name': 'widget', 'Nullable': 'UNKNOWN','Precision': 2147483647, 'Scale': 0,'SchemaName': '','TableName': '','Type': 'varchar'}]},'Rows': [{'Data': [{'VarCharValue': 'widget'}]}]}],'Info': ['Job Status: SUCCEEDED']},
        }

        good = op.validate(config, params).run(env, Response())

        assert ((expect[config.execution().client.name()], 200) == good.with_status())

        # invalid job_id
        params = OperatorParameters(parameter_string="job_id:bad_job_id")
        bad = op.validate(config, params).run(env, Response())


        expect = {
            'spark': {'Errors': ['Error from server (NotFound): pods "bad_job_id-driver" not found']},
            'athena': {'Errors': ['QueryExecution bad_job_id was not found', 'Job errored: Invalid Job: QueryExecution bad_job_id was not found']}
        }

        assert (bad.with_status() == (expect[config.execution().client.name()], 400))


    run_tests("job", "get", True, "fatal", ["5", "6"], tests)
