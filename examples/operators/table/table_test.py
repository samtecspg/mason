from unittest.mock import patch
from examples.operators.table.get import run as table_get
from examples.operators.table.list import run as table_list
from examples.operators.table.infer import run as table_infer
from examples.operators.table.refresh import run as table_refresh
from examples.operators.table.test.expects import table as expects
from parameters import Parameters
from test.support import testing_base as base

def test_index():
    config, op = base.before("table", "list")

    if config and op:
        assert(op.supported_clients == ["glue"])
        for client in op.supported_clients:
            with patch('botocore.client.BaseClient._make_api_call', new=base.get_mock(client).method):
                params = Parameters(parameters="database_name:crawler-poc")
                validate = params.validate(op)
                exists = table_list(config, params, validate)
                assert exists.with_status() == expects.index()

                # Database DNE
                params = Parameters(parameters="database_name:bad-database")
                validate = params.validate(op)
                dne = table_list(config, params, validate)
                assert(dne.with_status() == expects.index(False))

def test_get():
    config, op = base.before("table", "get")

    if config and op:
        assert(op.supported_clients == ["glue"])
        for client in op.supported_clients:
            with patch('botocore.client.BaseClient._make_api_call', new=base.get_mock(client).method):
                # Database and table Exist
                params = Parameters(parameters="database_name:crawler-poc,table_name:catalog_poc_data")
                validate = params.validate(op)

                exists = table_get(config, params, validate)
                assert(exists.with_status() == expects.get(1))

                # Database DNE
                params = Parameters(parameters="database_name:bad-database,table_name:catalog_poc_data")
                validate = params.validate(op)
                dne = table_get(config, params, validate)
                assert(dne.with_status() ==expects.get(2))

                # Table DNE
                params = Parameters(parameters="database_name:crawler-poc,table_name:bad-table")
                validate = params.validate(op)
                dne2 = table_get(config, params, validate)
                assert(dne2.with_status() ==expects.get(3))

def test_post():
    config, op = base.before("table", "infer")

    if config and op:
        assert (op.supported_clients == ["glue"])
        for client in op.supported_clients:
            with patch('botocore.client.BaseClient._make_api_call', new=base.get_mock(client).method):

                #  DNE
                params = Parameters(parameters="database_name:crawler-poc,schedule_name:test_crawler_new,storage_path:lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/")
                validate = params.validate(op)
                exists = table_infer(config, params, validate)
                assert(exists.with_status() == expects.post(False))

                # Exists
                params = Parameters(parameters="database_name:crawler-poc,schedule_name:test_crawler,storage_path:lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/")
                validate = params.validate(op)
                exists = table_infer(config, params, validate)
                assert(exists.with_status() == expects.post(True))


def test_refresh():
    config, op = base.before("table", "refresh")

    if config and op:
        assert (op.supported_clients == ["glue"])
        for client in op.supported_clients:
            with patch('botocore.client.BaseClient._make_api_call', new=base.get_mock(client).method):

                # valid refresh
                params = Parameters(parameters="table_name:catalog_poc_data,database_name:crawler-poc")
                validate = params.validate(op)
                refresh = table_refresh(config, params, validate)
                assert(refresh.with_status() == expects.refresh(False))

                # already refreshing
                params = Parameters(parameters="table_name:catalog_poc_data_refreshing,database_name:crawler-poc")
                validate = params.validate(op)
                refreshing = table_refresh(config, params, validate)
                assert(refreshing.with_status() == expects.refresh(True))


