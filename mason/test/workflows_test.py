import os
import shutil

from mason.workflows import workflows
from mason.definitions import from_root
from mason.test.support import testing_base as base
from mason.operators import operators

class TestWorkflows:

    def test_end_to_end(self):
        base.set_log_level("trace")
        mason_home = from_root("/test/.tmp/")

        if os.path.exists(mason_home):
            shutil.rmtree(mason_home)

        env = base.get_env(workflow_home="/test/.tmp/workflows/", operator_home="/test/.tmp/operators/", config_home="/test/.tmp/configs/")
        ops, errors = operators.list_operators(from_root("/test/support/operators/"))

        for operator in ops:
            operator.register_to(env.operator_home)

        workflows.register_workflows(from_root("/test/support/workflows/"), env)

        wf = workflows.get_workflow(env, "namespace1", "workflow1")

        if os.path.exists(mason_home):
            shutil.rmtree(mason_home)

