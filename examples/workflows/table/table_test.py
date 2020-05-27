import os

from clients.response import Response
from configurations.valid_config import ValidConfig
from definitions import from_root
from parameters import WorkflowParameters
from test.support.testing_base import run_tests

from util.environment import MasonEnvironment
from examples.operators.table.test.expects import table as expects # type: ignore
from workflows import Workflow


def test_post():

    def tests(env: MasonEnvironment, config: ValidConfig, wf: Workflow):
        # DNE
        # params = WorkflowParameters(parameter_path=from_root("/test/support/parameters/table_infer_parameters_1.yaml"))
        # dne = wf.validate(env, config, params).run(env, Response(), False, True, "test_crawler_new")
        # assert(dne.with_status() == expects.post(False))

        # Exists
        params = WorkflowParameters(parameter_path=from_root("/test/support/parameters/table_infer_parameters_2.yaml"))
        exists = wf.validate(env, config, params).run(env, Response(), False, True, "test_crawler")
        assert(exists.with_status() == expects.post(True))

        # API
        # response, status = table_infer_api(env, config, database_name="crawler-poc", schedule_name="test_crawler_new",storage_path="lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/", log_level="fatal")
        # assert((response, status) == expects.post(False))

    os.environ["GLUE_ROLE_ARN"] = "TestRole"
    run_tests("table", "infer", True, "fatal", ["config_1"], tests, workflow=True)

