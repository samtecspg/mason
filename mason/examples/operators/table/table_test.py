import shutil
from os import path

from dotenv import load_dotenv

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.definitions import from_root
from mason.engines.execution.models.jobs import ExecutedJob, InvalidJob
from mason.engines.metastore.models.table.invalid_table import InvalidTables
from mason.engines.metastore.models.table.summary import TableSummary
from mason.operators.operator import Operator
from mason.examples.operators.table.test.expects import table
from mason.parameters.operator_parameters import OperatorParameters
from mason.test.support.testing_base import run_tests, clean_path
from mason.util.environment import MasonEnvironment

load_dotenv(from_root("/../.env.example"), override=True)

def test_index():
    def tests(env: MasonEnvironment, config: Config, op: Operator):
        
        # Database Exists
        params = OperatorParameters(parameter_string="database_name:test-database")
        valid = op.validate(config, params)
        exists = valid.run(env, Response())
        assert exists.with_status() == table.index(config.metastore().client.name())

        # Database DNE
        params = OperatorParameters(parameter_string="database_name:bad-database")
        dne = op.validate(config, params).run(env, Response())
        assert(dne.with_status() == table.index(config.metastore().client.name(), False))

    run_tests("table", "list", True, "fatal", ["2", "3"], tests)

def test_get():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # Database and table Exist
        parameters = table.parameters(config.id)
        params = OperatorParameters(parameter_string=parameters[0])
        exists = op.validate(config, params).run(env, Response())
        assert(exists.with_status() == table.get(config.metastore().client.name(), 1))

        # Database DNE
        params = OperatorParameters(parameter_string=parameters[1])
        dne = op.validate(config, params).run(env, Response())
        assert(clean_path(dne.with_status()) == table.get(config.metastore().client.name(), 2))

        # Table DNE
        params = OperatorParameters(parameter_string=parameters[2])
        dne2 = op.validate(config, params).run(env, Response())
        assert(clean_path(dne2.with_status()) == table.get(config.metastore().client.name(), 3))

    run_tests("table", "get", True, "fatal", ["1", "2", "3"],  tests)


def test_refresh():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid refresh
        params = OperatorParameters(parameter_string="table_name:test-table,database_name:test-database")
        refresh = op.validate(config, params).run(env, Response())
        assert(refresh.with_status() == table.refresh(False))

        # already refreshing
        params = OperatorParameters(parameter_string="table_name:test-table_refreshing,database_name:test-database")
        refreshing = op.validate(config, params).run(env, Response())
        assert(refreshing.with_status() == table.refresh(True))

    run_tests("table", "refresh", True, "fatal", ["2"],  tests)

def test_merge():
    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # unsupported merge schema
        params = OperatorParameters(parameter_string="database_name:good_input_bucket,table_name:good_input_path,output_path:good_output_bucket/good_output_path,parse_headers:true")
        unsupported = op.validate(config, params).run(env, Response()).response
        assert('No conflicting schemas found at good_input_bucket,good_input_path. Merge unnecessary. ' in unsupported.formatted()["Errors"][0])

        # invalid merge params
        params = OperatorParameters(parameter_string="input_path:test,database_name:test,table_name:test")
        invalid = op.validate(config, params).run(env, Response())
        assert(invalid.with_status() == ({'Errors': ['Invalid Operator.  Reason:  Invalid parameters.  Required parameter not specified: output_path']}, 400))

        # valid merge
        params = OperatorParameters(parameter_string="database_name:good_input_bucket_2,table_name:good_input_path,output_path:good_output_bucket/good_output_path,parse_headers:true")
        valid = op.validate(config, params).run(env, Response())
        expect = ({'Data': [{'Logs': ['sparkapplication.sparkoperator.k8s.io/mason-spark-merge created']}],
          'Info': ['Fetching keys at s3://good_input_bucket_2/good_input_path'],
          'Warnings': ['Sampling keys to determine schema. Sample size: 3.']}, 200)
        assert(valid.with_status() == expect)

    run_tests("table", "merge", True, "fatal", ["5"],  tests)

def test_query():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid query
        query = "SELECT * from $table limit 3"
        output_path = from_root("/.tmp/")
        params = OperatorParameters(parameter_string=f"query_string:{query},database_name:good_database,table_name:good_table,output_path:{output_path}")
        result = op.validate(config, params).run(env, Response())
        exp = {
            "6": ['Running Query "SELECT * from $table limit 3"', 'Running Athena query.  query_id: test'],
            "4": [f'Table succesfully formatted as parquet and exported to {output_path}']
        }

        assert((result.formatted()["Info"], result.status_code()) == (exp[config.id], 200))

        # bad permissions
        query = "SELECT * from $table limit 3"
        params = OperatorParameters(parameter_string=f"query_string:{query},database_name:access_denied,table_name:good_table,output_path:{output_path}")
        result = op.validate(config, params).run(env, Response())
        exp_2 = {
            "6": ({'Errors': ['Job errored: Access denied for credentials.  Ensure associated user or role has permission to CreateNamedQuery on athena'], 'Info': ['Running Query "SELECT * from $table limit 3"']}, 403),
            "4": ({'Info': [f'Table succesfully formatted as parquet and exported to {output_path}']}, 200)
        }

        assert(result.with_status() == exp_2[config.id])

    run_tests("table", "query", True, "fatal", ["4", "6"], tests)

    tmp_folder = from_root("/.tmp/")
    if path.exists(tmp_folder):
        shutil.rmtree(tmp_folder)


def test_delete():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid delete
        params = OperatorParameters(parameter_string=f"table_name:good_table,database_name:good_database")
        good = op.validate(config, params).run(env, Response())
        assert(good.with_status() == ({'Info': ['Table good_table successfully deleted.']}, 200))

        # database DNE
        params = OperatorParameters(parameter_string=f"table_name:bad_table,database_name:bad_database")
        bad = op.validate(config, params).run(env, Response())
        assert(bad.with_status() == ({'Errors': ['Database bad_database not found.']}, 400))

        # table DNE
        params = OperatorParameters(parameter_string=f"table_name:bad_table,database_name:good_database")
        bad = op.validate(config, params).run(env, Response())
        assert(bad.with_status() == ({'Errors': ['Table bad_table not found.']}, 400))


    run_tests("table", "delete", True, "fatal", ["2"], tests)

def test_infer():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # database DNE
        params = OperatorParameters(parameter_string=f"database_name:bad-database,storage_path:test-database/test-table")
        good = op.validate(config, params).run(env, Response())

        assert(good.with_status() == ({'Errors': ['Job errored: Metastore database bad-database not found'], 'Info': ['Fetching keys at s3://test-database/test-table', 'Table inferred: test-table', 'Fetching Database: bad-database'], 'Warnings': ['Sampling keys to determine schema. Sample size: 3.']}, 404))

        # bad path
        params = OperatorParameters(parameter_string=f"database_name:test-database,storage_path:test-database/bad-table")
        good = op.validate(config, params).run(env, Response())
        assert(good.with_status() == ({  'Info': ['Fetching keys at s3://test-database/bad-table'], 'Errors': ['No keys at s3://test-database/bad-table', 'Job errored: Invalid Tables: No keys at s3://test-database/bad-table'], 'Warnings': ['Sampling keys to determine schema. Sample size: 3.']}, 404))

        # valid path
        params = OperatorParameters(parameter_string=f"database_name:test-database,storage_path:test-database/test-table,output_path:test-database/athena/")
        good = op.validate(config, params).run(env, Response())

        result = good.formatted()
        expect = {'Data': [{'Logs': ['Running job id=test_id']}],
                  'Info': ['Fetching keys at s3://test-database/test-table',
                  'Table inferred: test-table',
                  'Fetching Database: test-database',
                  'Running Athena query.  query_id: test_id'],
         'Warnings': ['Sampling keys to determine schema. Sample size: 3.']}

        assert(result == expect)

    run_tests("table", "infer", True, "fatal", ["6"], tests)


def test_format():


    def tests(env: MasonEnvironment, config: Config, op: Operator):
        params = OperatorParameters(parameter_string=f"database_name:mason-sample-data,table_name:tests/in/csv/,format:boogo,output_path:mason-sample-data/tests/out/csv/")
        bad = op.validate(config, params).run(env, Response())
        invalid_job = bad.object
        assert(isinstance(invalid_job, InvalidJob))

        params = OperatorParameters(parameter_string=f"database_name:mason-sample-data,table_name:tests/in/csv/,format:csv,output_path:good_output_path")
        good = op.validate(config, params).run(env, Response())
        executed_job = good.object
        assert(isinstance(executed_job, ExecutedJob))

    run_tests("table", "format", True, "fatal", ["4"], tests)
    
def test_summarize():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        parameters = table.parameters(config.id)[1]
        params = OperatorParameters(parameter_string=parameters)
        bad = op.validate(config, params).run(env, Response())
        invalid_tables = bad.object
        assert(isinstance(invalid_tables, InvalidTables))
        # assert(isinstance(invalid_tables.invalid_tables[0], TableNotFound))

        parameters = table.parameters(config.id)[0]
        params = OperatorParameters(parameter_string=parameters)
        good = op.validate(config, params).run(env, Response())
        summary = good.object
        assert(isinstance(summary, TableSummary))

    run_tests("table", "summarize", True, "fatal", ["1", "3"], tests)
    
def test_summarize_async():

    load_dotenv(from_root("/../.env"), override=True)

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # good
        parameters = table.parameters(config.id)[0]
        params = OperatorParameters(parameter_string=parameters)
        good = op.validate(config, params).run(env, Response())
        assert(isinstance(good.object, ExecutedJob))

        parameters = table.parameters(config.id)[1]
        params = OperatorParameters(parameter_string=parameters)
        bad = op.validate(config, params).run(env, Response())
        assert(isinstance(bad.object, InvalidJob))

    run_tests("table", "summarize", True, "fatal", ["5"], tests)

