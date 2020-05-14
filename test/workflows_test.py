import os
import shutil

from definitions import from_root
from parameters import InputParameters
from test.support import testing_base as base
import workflows as Workflows
from operators import operators as Operators
from util.logger import logger


class TestWorkflows:

    def test_end_to_end(self):
        base.set_log_level("trace")
        mason_home = from_root("/test/.tmp/")

        if os.path.exists(mason_home):
            shutil.rmtree(mason_home)

        env = base.get_env(workflow_home="/test/.tmp/workflows/", operator_home="/test/.tmp/operators/", config_home="/test/.tmp/configs/")
        operators, errors = Operators.list_operators(from_root("/test/support/operators/"))

        for operator in operators:
            operator.register_to(env.operator_home)

        Workflows.register_workflows(from_root("/test/support/workflows/"), env)

        wf = Workflows.get_workflow(env, "namespace1", "workflow1")

        if os.path.exists(mason_home):
            shutil.rmtree(mason_home)

