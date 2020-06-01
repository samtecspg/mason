import os

from mason.clients.response import Response
from mason.configurations.valid_config import ValidConfig
from mason.definitions import from_root
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.test.support.testing_base import run_tests

from mason.util.environment import MasonEnvironment
from mason.examples.operators.table.test.expects import table as expects
from mason.examples.workflows.table.infer import api as table_infer_api
from mason.workflows.workflow import Workflow


def test_post():

    def tests(env: MasonEnvironment, config: ValidConfig, wf: Workflow):
        # DNE
        params = WorkflowParameters(parameter_path=from_root("/test/support/parameters/table_infer_parameters_1.yaml"))
        dne = wf.validate(env, config, params).run(env, Response(), False, True, "test_crawler_new")
        assert(dne.with_status() == expects.post(False))

        # Exists
        params = WorkflowParameters(parameter_path=from_root("/test/support/parameters/table_infer_parameters_2.yaml"))
        exists = wf.validate(env, config, params).run(env, Response(), False, True, "test_crawler")
        assert(exists.with_status() == expects.post(True))

        # API
        parameter_dict = \
        { "step_1": {
            "config_id": 1,
            "parameters":
                {"database_name": "crawler-poc", "storage_path": "lake-working-copy-feb-20-2020/user-data/kyle.prifogle/catalog_poc_data/"}
            }
        }


        response, status = table_infer_api(env, config,  parameters=parameter_dict, log_level="fatal", deploy=True, run_now=True, schedule_name="test_crawler_new")
        assert((response, status) == expects.post(False))

    os.environ["GLUE_ROLE_ARN"] = "TestRole"
    run_tests("table", "infer", True, "fatal", ["config_1"], tests, workflow=True)

