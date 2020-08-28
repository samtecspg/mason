from typing import List
from dotenv import load_dotenv
import os

from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.definitions import from_root
from mason.examples.operators.table.get import api as table_get_api
from mason.examples.operators.table.list import api as table_list_api
from mason.examples.operators.table.refresh import api as table_refresh_api
from mason.examples.operators.table.infer import api as table_infer_api
from mason.examples.operators.table.test.expects import table as expects
from mason.operators.operator import Operator
from mason.parameters.input_parameters import InputParameters
from mason.test.support.testing_base import run_tests, clean_string, clean_uuid
from mason.util.environment import MasonEnvironment
from mason.util.logger import logger

load_dotenv(from_root("/../.env.example"), override=True)

def test_index():
    def tests(env: MasonEnvironment, config: ValidConfig, op: Operator):
        # Database Exists
        params = InputParameters(parameter_string="database_name:crawler-poc")
        exists = op.validate(config, params).run(env, Response())
        assert exists.with_status() == expects.index(config.metastore.client_name)

        # Database DNE
        params = InputParameters(parameter_string="database_name:bad-database")
        dne = op.validate(config, params).run(env, Response())
        assert(dne.with_status() == expects.index(config.metastore.client_name, False))

        # Api
        response, status = table_list_api(env, config, database_name="crawler-poc", log_level="fatal")
        assert((response, status) == expects.index(config.metastore.client_name))

    run_tests("table", "list", True, "fatal", ["config_1", "config_2"], tests)

def test_get():

    def tests(env: MasonEnvironment, config: ValidConfig, op: Operator):
        # Database and table Exist
        params = InputParameters(parameter_string="database_name:crawler-poc,table_name:catalog_poc_data")
        exists = op.validate(config, params).run(env, Response())
        assert(exists.with_status() == expects.get(config.metastore.client_name, 1))

        # Database DNE
        params = InputParameters(parameter_string="database_name:bad-database,table_name:catalog_poc_data")
        dne = op.validate(config, params).run(env, Response())
        assert(dne.with_status() ==expects.get(config.metastore.client_name, 2))

        # Table DNE
        params = InputParameters(parameter_string="database_name:crawler-poc,table_name:bad-table")
        dne2 = op.validate(config,params).run(env, Response())
        assert(dne2.with_status() == expects.get(config.metastore.client_name, 3))

        # API
        response, status = table_get_api(env, config, database_name="crawler-poc", table_name="catalog_poc_data", log_level="fatal")
        assert((response, status) == expects.get(config.metastore.client_name, 1))

    run_tests("table", "get", True, "fatal", ["config_1", "config_2"],  tests)


def test_refresh():

    def tests(env: MasonEnvironment, config: ValidConfig, op: Operator):
        # valid refresh
        params = InputParameters(parameter_string="table_name:catalog_poc_data,database_name:crawler-poc")
        refresh = op.validate(config, params).run(env, Response())
        assert(refresh.with_status() == expects.refresh(False))

        # already refreshing
        params = InputParameters(parameter_string="table_name:catalog_poc_data_refreshing,database_name:crawler-poc")
        refreshing = op.validate(config, params).run(env, Response())
        assert(refreshing.with_status() == expects.refresh(True))

        # API
        response, status = table_refresh_api(env, config, table_name="catalog_poc_data", database_name="crawler-poc", log_level="fatal")
        assert((response, status) == expects.refresh(False))

    run_tests("table", "refresh", True, "fatal", ["config_1"],  tests)


def test_merge():

    def tests(env: MasonEnvironment, config: ValidConfig, op: Operator):
        # unsupported merge schema
        params = InputParameters(parameter_string="input_path:good_input_bucket/good_input_path,output_path:good_output_bucket/good_output_path,parse_headers:true")
        unsupported = op.validate(config, params).run(env, Response()).response
        assert('No conflicting schemas found at good_input_bucket/good_input_path. Merge unecessary. ' in unsupported.formatted()["Errors"][0])

        # invalid merge params
        # params = InputParameters(parameter_string="input_path:test,bad:test")
        # invalid = op.validate(config, params).run(env, Response())
        # assert(invalid.with_status() == ({'Errors': ['Invalid Operator.  Reason:  Invalid parameters.  Required parameter not specified: output_path'], 'Info': [], 'Warnings': []}, 400))

        # TODO: fix
        # valid merge
        # params = InputParameters(parameter_string="input_path:good_input_bucket_2/good_input_path,output_path:good_output_bucket/good_output_path,parse_headers:true")
        # valid = op.validate(config, params).run(env, Response())
        # expect = ({'Data': [{'Schema': {'Columns': [{'ConvertedType': 'REQUIRED', 'Name': 'test_column_1',
        #                                    'RepititionType': None,
        #                                    'Type': 'INT32'},
        #                                   {'ConvertedType': 'UTF8',
        #                                    'Name': 'test_column_2',
        #                                    'RepititionType': 'OPTIONAL',
        #                                    'Type': 'BYTE_ARRAY'}],
        #                       'SchemaType': 'parquet'}},
        #     {'Logs': ['sparkapplication.sparkoperator.k8s.io/mason-spark-merge- created']}],
        #   'Errors': [],
        #   'Info': ['Running job id=merge'],
        #   'Warnings': []},
        #  200)
        # assert(valid.with_status() == expect)

    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    run_tests("table", "merge", True, "fatal", ["config_4"],  tests)

def test_query():

    def tests(env: MasonEnvironment, config: ValidConfig, op: Operator):
        # valid query
        query = "SELECT * from good_table limit 5"
        params = InputParameters(parameter_string=f"query_string:{query},database_name:good_database")
        result = op.validate(config, params).run(env, Response())
        expect = {'Errors': [], 'Info': ['Running Query "SELECT * from good_table limit 5"', 'Running Athena query.  query_id: test', 'Running job id=test'], 'Warnings': []}

        assert(result.with_status() == (expect, 200))

        # bad permissions
        query = "SELECT * from good_table limit 5"
        params = InputParameters(parameter_string=f"query_string:{query},database_name:access_denied")
        result = op.validate(config, params).run(env, Response())
        expect = {'Errors': ['Job errored: Access denied for credentials.  Ensure associated user or role has permission to CreateNamedQuery on athena'], 'Info': ['Running Query "SELECT * from good_table limit 5"'], 'Warnings': []}
        assert(result.with_status() == (expect, 403))

    run_tests("table", "query", True, "fatal", ["config_3"], tests)

def test_delete():


    def tests(env: MasonEnvironment, config: ValidConfig, op: Operator):
        # valid delete
        params = InputParameters(parameter_string=f"table_name:good_table,database_name:good_database")
        good = op.validate(config, params).run(env, Response())
        assert(good.with_status() == ({'Errors': [], 'Info': ['Table good_table successfully deleted.'], 'Warnings': []}, 200))

        # database DNE
        params = InputParameters(parameter_string=f"table_name:bad_table,database_name:bad_database")
        bad = op.validate(config, params).run(env, Response())
        assert(bad.with_status() == ({'Errors': ['Database bad_database not found.'], 'Info': [], 'Warnings': []}, 400))

        # table DNE
        params = InputParameters(parameter_string=f"table_name:bad_table,database_name:good_database")
        bad = op.validate(config, params).run(env, Response())
        assert(bad.with_status() == ({'Errors': ['Table bad_table not found.'], 'Info': [], 'Warnings': []}, 400))


    run_tests("table", "delete", True, "fatal", ["config_1"], tests)

def test_infer():

    def tests(env: MasonEnvironment, config: ValidConfig, op: Operator):

        # database DNE
        params = InputParameters(parameter_string=f"database_name:bad-database,storage_path:crawler-poc/catalog_poc_data")
        good = op.validate(config, params).run(env, Response())
        assert(good.with_status() == ({'Errors': ['Job errored: Metastore database bad-database not found'], 'Info': ['Table inferred: catalog_poc_data'], 'Warnings': []}, 404))

        # bad path
        params = InputParameters(parameter_string=f"database_name:crawler-poc,storage_path:crawler-poc/bad-table")
        good = op.validate(config, params).run(env, Response())
        assert(good.with_status() == ({'Errors': ['No keys at s3://crawler-poc/bad-table', 'Job errored: Invalid Tables: No keys at s3://crawler-poc/bad-table'], 'Info': [], 'Warnings': []}, 404))

         # valid path
        params = InputParameters(parameter_string=f"database_name:crawler-poc,storage_path:crawler-poc/catalog_poc_data,output_path:crawler-poc/athena/")
        good = op.validate(config, params).run(env, Response())
        def clean(s: List[str]):
            return list(map(lambda i: clean_uuid(clean_string(i)), s))

        infos = clean(good.formatted()["Info"])

        expect = [
            'Tableinferred:catalog_poc_data',
            'RunningAthenaquery.query_id:test_id',
            'Runningjobid=test_id'
        ]
        assert(infos == expect)

        # API
        response, status = table_infer_api(env, config, database_name="crawler-poc", storage_path="crawler-poc/catalog_poc_data", output_path="crawler-poc/athena/", log_level="fatal")
        assert (clean(response["Errors"]) == [])
        assert (clean(response["Info"]) == expect)

    run_tests("table", "infer", True, "fatal", ["config_5"], tests)


def test_format():
    
    load_dotenv(from_root("/../.env"), override=True)
    
    def tests(env: MasonEnvironment, config: ValidConfig, op: Operator):
        params = InputParameters(parameter_string=f"database_name:mason-sample-data,table_name:tests/in/csv/,format:boogo,output_path:mason-sample-data/tests/out/csv/")
        good = op.validate(config, params).run(env, Response())
        print(good.response.formatted())
        
        # InvalidJob
        # OSError: Timed out trying to connect to 'tcp://dask-scheduler:8786'
        # ExecutedJob 'Table succesfully formatted as csv and exported to s3://mason-sample-data/tests/out/csv/'
        
        logger.remove("HERE")

    run_tests("table", "format", False, "fatal", ["config_6"], tests)
