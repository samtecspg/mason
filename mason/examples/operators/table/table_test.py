import shutil
from os import path
from typing import List

from dotenv import load_dotenv

from mason.clients.response import Response
from mason.configurations.config import Config
from mason.definitions import from_root
from mason.engines.execution.models.jobs import InvalidJob, ExecutedJob
from mason.operators.operator import Operator
from mason.examples.operators.table.test.expects import table
from mason.parameters.operator_parameters import OperatorParameters
from mason.test.support.testing_base import run_tests, clean, clean_path
from mason.util.environment import MasonEnvironment

load_dotenv(from_root("/../.env.example"), override=True)

def test_index():
    def tests(env: MasonEnvironment, config: Config, op: Operator):
        
        # Database Exists
        params = OperatorParameters(parameter_string="database_name:crawler-poc")
        valid = op.validate(config, params)
        exists = valid.run(env, Response())
        assert exists.with_status() == table.index(config.metastore().client.name())

        # Database DNE
        params = OperatorParameters(parameter_string="database_name:bad-database")
        dne = op.validate(config, params).run(env, Response())
        assert(dne.with_status() == table.index(config.metastore().client.name(), False))

    # run_tests("table", "list", True, "fatal", ["1", "2"], tests)
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
        params = OperatorParameters(parameter_string="table_name:catalog_poc_data,database_name:crawler-poc")
        refresh = op.validate(config, params).run(env, Response())
        assert(refresh.with_status() == table.refresh(False))

        # already refreshing
        params = OperatorParameters(parameter_string="table_name:catalog_poc_data_refreshing,database_name:crawler-poc")
        refreshing = op.validate(config, params).run(env, Response())
        assert(refreshing.with_status() == table.refresh(True))

    run_tests("table", "refresh", True, "fatal", ["1"],  tests)

def test_merge():
    pass
    # TODO: fix
    # def tests(env: MasonEnvironment, config: Config, op: Operator):
    #     # unsupported merge schema
    #     params = OperatorParameters(parameter_string="input_path:good_input_bucket/good_input_path,output_path:good_output_bucket/good_output_path,parse_headers:true")
    #     unsupported = op.validate(config, params).run(env, Response()).response
    #     assert('No conflicting schemas found at good_input_bucket/good_input_path. Merge unecessary. ' in unsupported.formatted()["Errors"][0])
    # 
    #     # invalid merge params
    #     # params = InputParameters(parameter_string="input_path:test,bad:test")
    #     # invalid = op.validate(config, params).run(env, Response())
    #     # assert(invalid.with_status() == ({'Errors': ['Invalid Operator.  Reason:  Invalid parameters.  Required parameter not specified: output_path'], 'Info': [], 'Warnings': []}, 400))
    # 
    #     # valid merge
    #     # params = InputParameters(parameter_string="input_path:good_input_bucket_2/good_input_path,output_path:good_output_bucket/good_output_path,parse_headers:true")
    #     # valid = op.validate(config, params).run(env, Response())
    #     # expect = ({'Data': [{'Schema': {'Columns': [{'ConvertedType': 'REQUIRED', 'Name': 'test_column_1',
    #     #                                    'RepititionType': None,
    #     #                                    'Type': 'INT32'},
    #     #                                   {'ConvertedType': 'UTF8',
    #     #                                    'Name': 'test_column_2',
    #     #                                    'RepititionType': 'OPTIONAL',
    #     #                                    'Type': 'BYTE_ARRAY'}],
    #     #                       'SchemaType': 'parquet'}},
    #     #     {'Logs': ['sparkapplication.sparkoperator.k8s.io/mason-spark-merge- created']}],
    #     #   'Errors': [],
    #     #   'Info': ['Running job id=merge'],
    #     #   'Warnings': []},
    #     #  200)
    #     # assert(valid.with_status() == expect)
    # 
    # environ["AWS_SECRET_ACCESS_KEY"] = "test"
    # environ["AWS_ACCESS_KEY_ID"] = "test"
    # run_tests("table", "merge", True, "fatal", ["config_4"],  tests)

def test_query():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid query
        query = "SELECT * from $table limit 3"
        output_path = from_root("/.tmp/")
        params = OperatorParameters(parameter_string=f"query_string:{query},database_name:good_database,table_name:good_table,output_path:{output_path}")
        result = op.validate(config, params).run(env, Response())
        exp = {
            "1": ['Running Query "SELECT * from $table limit 3"', 'Running Athena query.  query_id: test', 'Running job id=test'],
            "4": [f'Table succesfully formatted as parquet and exported to {output_path}']
        }

        expect = {'Info': exp[config.id]}
        assert(result.with_status() == (expect, 200))

        # bad permissions
        query = "SELECT * from $table limit 3"
        params = OperatorParameters(parameter_string=f"query_string:{query},database_name:access_denied,table_name:good_table,output_path:{output_path}")
        result = op.validate(config, params).run(env, Response())
        exp_2 = {
            "1": ({'Errors': ['Job errored: Access denied for credentials.  Ensure associated user or role has permission to CreateNamedQuery on athena'], 'Info': ['Running Query "SELECT * from $table limit 3"']}, 403),
            "4": ({'Info': [f'Table succesfully formatted as parquet and exported to {output_path}']}, 200)
        }

        assert(result.with_status() == exp_2[config.id])


    run_tests("table", "query", True, "fatal", ["1", "4"], tests)

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


    run_tests("table", "delete", True, "fatal", ["1"], tests)

def test_infer():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # database DNE
        params = OperatorParameters(parameter_string=f"database_name:bad-database,storage_path:crawler-poc/catalog_poc_data")
        good = op.validate(config, params).run(env, Response())
        assert(good.with_status() == ({'Errors': ['Job errored: Metastore database bad-database not found'], 'Info': ['Table inferred: catalog_poc_data']}, 404))

        # bad path
        params = OperatorParameters(parameter_string=f"database_name:crawler-poc,storage_path:crawler-poc/bad-table")
        good = op.validate(config, params).run(env, Response())
        assert(good.with_status() == ({'Errors': ['No keys at s3://crawler-poc/bad-table', 'Job errored: Invalid Tables: No keys at s3://crawler-poc/bad-table']}, 404))

        # valid path
        params = OperatorParameters(parameter_string=f"database_name:crawler-poc,storage_path:crawler-poc/catalog_poc_data,output_path:crawler-poc/athena/")
        good = op.validate(config, params).run(env, Response())

        infos = clean(good.formatted()["Info"])
        expect = [
            'Tableinferred:catalog_poc_data',
            'RunningAthenaquery.query_id:test_id',
            'Runningjobid=test_id'
        ]
        assert(infos == expect)

    run_tests("table", "infer", True, "fatal", ["3"], tests)


def test_format():

    load_dotenv(from_root("/../.env"), override=True)

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        params = OperatorParameters(parameter_string=f"database_name:mason-sample-data,table_name:tests/in/csv/,format:boogo,output_path:mason-sample-data/tests/out/csv/")
        good = op.validate(config, params).run(env, Response())
        invalid_job = good.object
        assert(isinstance(invalid_job, InvalidJob))

        params = OperatorParameters(parameter_string=f"database_name:mason-sample-data,table_name:tests/in/csv/,format:csv,output_path:good_output_path")
        good = op.validate(config, params).run(env, Response())
        executed_job = good.object
        assert(isinstance(executed_job, ExecutedJob))

    run_tests("table", "format", True, "fatal", ["4"], tests)
