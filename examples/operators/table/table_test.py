from examples.operators.table.list import run as table_list
from examples.operators.table.get import run as table_get
from examples.operators.table.infer import run as table_infer
from examples.operators.table.refresh import run as table_refresh
from examples.operators.table.merge import run as table_merge

from examples.operators.table.list import api as table_list_api
from examples.operators.table.refresh import api as table_refresh_api
from examples.operators.table.infer import api as table_infer_api


from examples.operators.table.test.expects import table as expects # type: ignore
from parameters import Parameters
from configurations import Config
from operators.operator import Operator
from test.support.testing_base import run_tests
from util.environment import MasonEnvironment


def test_index():
    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # Database Exists
        params = Parameters(parameters="database_name:crawler-poc")
        validate = params.validate(op)
        exists = table_list(env, config, params, validate)
        assert exists.with_status() == expects.index(config.metastore.client_name)

        # Database DNE
        params = Parameters(parameters="database_name:bad-database")
        validate = params.validate(op)
        dne = table_list(env, config, params, validate)
        assert(dne.with_status() == expects.index(config.metastore.client_name, False))

        # Api
        response, status = table_list_api(env, config, database_name="crawler-poc")
        assert((response, status) == expects.index(config.metastore.client_name))

    run_tests("table", "list", tests)

def test_get():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # Database and table Exist
        params = Parameters(parameters="database_name:crawler-poc,table_name:catalog_poc_data")
        validate = params.validate(op)

        exists = table_get(env, config, params, validate)
        assert(exists.with_status() == expects.get(config.metastore.client_name, 1))

        # Database DNE
        params = Parameters(parameters="database_name:bad-database,table_name:catalog_poc_data")
        validate = params.validate(op)
        dne = table_get(env, config, params, validate)
        assert(dne.with_status() ==expects.get(config.metastore.client_name, 2))

        # Table DNE
        params = Parameters(parameters="database_name:crawler-poc,table_name:bad-table")
        validate = params.validate(op)
        dne2 = table_get(env, config, params, validate)
        assert(dne2.with_status() == expects.get(config.metastore.client_name, 3))

        # # API
        # response, status = table_get_api(env, config, database_name="crawler-poc", table_name="catalog_poc_data")
        # assert((response, status) == expects.get(1))

    run_tests("table", "get", tests)

def test_post():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        #  DNE
        params = Parameters(parameters="database_name:crawler-poc,schedule_name:test_crawler_new,storage_path:lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/")
        validate = params.validate(op)
        exists = table_infer(env, config, params, validate)
        assert(exists.with_status() == expects.post(False))

        # Exists
        params = Parameters(parameters="database_name:crawler-poc,schedule_name:test_crawler,storage_path:lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/")
        validate = params.validate(op)
        exists = table_infer(env, config, params, validate)
        assert(exists.with_status() == expects.post(True))

        # API
        response, status = table_infer_api(env, config, database_name="crawler-poc", schedule_name="test_crawler_new",storage_path="lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/")
        assert((response, status) == expects.post(False))

    run_tests("table", "infer", tests)



def test_refresh():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid refresh
        params = Parameters(parameters="table_name:catalog_poc_data,database_name:crawler-poc")
        validate = params.validate(op)
        refresh = table_refresh(env, config, params, validate)
        assert(refresh.with_status() == expects.refresh(False))

        # already refreshing
        params = Parameters(parameters="table_name:catalog_poc_data_refreshing,database_name:crawler-poc")
        validate = params.validate(op)
        refreshing = table_refresh(env, config, params, validate)
        assert(refreshing.with_status() == expects.refresh(True))

        # API
        response, status = table_refresh_api(env, config, table_name="catalog_poc_data", database_name="crawler-poc")
        assert((response, status) == expects.refresh(False))

    run_tests("table", "refresh", tests)


def test_merge():

    def tests(env: MasonEnvironment, config: Config, op: Operator):
        # valid refresh
        params = Parameters(parameters="merge_strategy:test")
        validate = params.validate(op)
        merge = table_merge(env, config, params, validate)
        assert(merge.with_status() == expects.refresh(False))

    run_tests("table", "merge", tests)
