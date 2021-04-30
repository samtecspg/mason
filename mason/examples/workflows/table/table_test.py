from mason.clients.response import Response
from mason.configurations.config import Config
from mason.definitions import from_root
from mason.parameters.workflow_parameters import WorkflowParameters
from mason.test.support.testing_base import run_tests

from mason.util.environment import MasonEnvironment
from mason.examples.operators.table.test.expects import table as expects
from mason.workflows.workflow import Workflow


def test_post():

    def tests(env: MasonEnvironment, config: Config, wf: Workflow):
        # DNE
        params = WorkflowParameters(parameter_path=from_root("/test/support/parameters/table_infer_parameters_1.yaml"))
        dne = wf.validate(env, config, params).run(env, Response())
        assert(dne.with_status() == expects.post(False))
        
        # Exists
        params = WorkflowParameters(parameter_path=from_root("/test/support/parameters/table_infer_parameters_2.yaml"))
        exists = wf.validate(env, config, params).run(env, Response())
        assert(exists.with_status() == expects.post(True))

    run_tests("table", "infer", True, "fatal", ["2"], tests, workflow=True)

