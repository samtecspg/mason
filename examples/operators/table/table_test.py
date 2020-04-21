from examples.operators.table.get import api as table_get_api
from examples.operators.table.list import api as table_list_api
from examples.operators.table.refresh import api as table_refresh_api
from examples.operators.table.infer import api as table_infer_api

from clients.response import Response
from examples.operators.table.test.expects import table as expects # type: ignore
from parameters import Parameters
from configurations import Config
from operators.operator import Operator
from test.support.testing_base import run_tests
from util.environment import MasonEnvironment

import os


def test_index():
    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # Database Exists
        params = Parameters(parameters="database_name:crawler-poc")
        exists = op.run(env, config, params, Response())
        assert exists.with_status() == expects.index(config.metastore.client_name)

        # Database DNE
        params = Parameters(parameters="database_name:bad-database")
        dne = op.run(env, config, params, Response())
        assert(dne.with_status() == expects.index(config.metastore.client_name, False))

        # Api
        response, status = table_list_api(env, config, database_name="crawler-poc")
        assert((response, status) == expects.index(config.metastore.client_name))

    run_tests("table", "list", True, tests)

def test_get():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # Database and table Exist
        params = Parameters(parameters="database_name:crawler-poc,table_name:catalog_poc_data")
        exists = op.run(env, config, params, Response())
        assert(exists.with_status() == expects.get(config.metastore.client_name, 1))

        # Database DNE
        params = Parameters(parameters="database_name:bad-database,table_name:catalog_poc_data")
        dne = op.run(env, config, params, Response())
        assert(dne.with_status() ==expects.get(config.metastore.client_name, 2))

        # Table DNE
        params = Parameters(parameters="database_name:crawler-poc,table_name:bad-table")
        dne2 = op.run(env, config, params, Response())
        assert(dne2.with_status() == expects.get(config.metastore.client_name, 3))

        # API
        response, status = table_get_api(env, config, database_name="crawler-poc", table_name="catalog_poc_data")
        assert((response, status) == expects.get(config.metastore.client_name, 1))

    run_tests("table", "get", True, tests)

def test_post():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        #  DNE
        params = Parameters(parameters="database_name:crawler-poc,schedule_name:test_crawler_new,storage_path:lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/")
        dne = op.run(env, config, params, Response())
        assert(dne.with_status() == expects.post(False))

        # Exists
        params = Parameters(parameters="database_name:crawler-poc,schedule_name:test_crawler,storage_path:lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/")
        exists = op.run(env, config, params, Response())
        assert(exists.with_status() == expects.post(True))

        # API
        response, status = table_infer_api(env, config, database_name="crawler-poc", schedule_name="test_crawler_new",storage_path="lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/")
        assert((response, status) == expects.post(False))

    run_tests("table", "infer", True,  tests)



def test_refresh():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid refresh
        params = Parameters(parameters="table_name:catalog_poc_data,database_name:crawler-poc")
        refresh = op.run(env, config, params, Response())
        assert(refresh.with_status() == expects.refresh(False))

        # already refreshing
        params = Parameters(parameters="table_name:catalog_poc_data_refreshing,database_name:crawler-poc")
        refreshing = op.run(env, config, params, Response())
        assert(refreshing.with_status() == expects.refresh(True))

        # API
        response, status = table_refresh_api(env, config, table_name="catalog_poc_data", database_name="crawler-poc")
        assert((response, status) == expects.refresh(False))

    run_tests("table", "refresh", True, tests)


def test_merge():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # unsupported merge schema
        os.environ["AWS_SECRET_ACCESS_KEY"]="test"
        os.environ["AWS_ACCESS_KEY_ID"]="test"
        params = Parameters(parameters="input_path:good_input_bucket/good_input_path,output_path:good_output_bucket/good_output_path,parse_headers:true")
        unsupported = op.run(env, config, params, Response())
        info = ['sparkapplication.sparkoperator.k8s.io/mason-spark-merge- created', 'Running job merge']
        expect = ({'Data': {'SchemaConflicts': {'CountDistinctSchemas': 2, 'DistinctSchemas': [{'Columns': [{'Name': 'type', 'Type': 'string'}, {'Name': 'price', 'Type': 'number'}],'SchemaType': 'text'}, {'Columns': [{'Name': 'type', 'Type': 'string'}, {'Name': 'price', 'Type': 'number'},{'Name': 'availabile','Type': 'boolean'},{'Name': 'date', 'Type': 'date'}],'SchemaType': 'text'}], 'NonOverlappingColumns': ['price', 'type']}}, 'Errors': [],'Info': info,'Warnings': []}, 200)
        assert(unsupported.with_status() == expect)

        # invalid merge params
        params = Parameters(parameters="input_path:test,bad:test")
        invalid = op.run(env, config, params, Response())
        assert(invalid.with_status() == ({'Errors': ['Missing required parameters: output_path'], 'Info': [], 'Warnings': []}, 400))

        # valid merge
        params = Parameters(parameters="input_path:good_input_bucket_2/good_input_path,output_path:good_output_bucket/good_output_path,parse_headers:true")
        valid = op.run(env, config, params, Response())
        expect = ({'Errors': [],
         'Info': ['sparkapplication.sparkoperator.k8s.io/mason-spark-merge- created', 'Running job merge'],
         'Warnings': [], 'Data': {'Schema': {'SchemaType': 'parquet', 'Columns': [
            {'Name': 'test_column_1', 'Type': 'INT32', 'ConvertedType': 'REQUIRED', 'RepititionType': None},
            {'Name': 'test_column_2', 'Type': 'BYTE_ARRAY', 'ConvertedType': 'UTF8', 'RepititionType': 'OPTIONAL'}]}}}, 200)

        assert(valid.with_status() == expect)


    run_tests("table", "merge", True, tests)
