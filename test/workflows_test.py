import os
import shutil

from definitions import from_root
from test.support import testing_base as base
import workflows as Workflows
import operators as Operators

class TestWorkflows:

    def test_end_to_end(self):
        base.set_log_level("fatal")
        mason_home = from_root("/test/.tmp/")


        if os.path.exists(mason_home):
            shutil.rmtree(mason_home)

        env = base.get_env(workflow_home="/test/.tmp/workflows/", operator_home="/test/.tmp/operators/", config_home="/test/.tmp/configs/")
        operators, errors = Operators.validate_operators(from_root("/test/support/operators/"), True)

        for operator in operators:
            operator.register_to(env.operator_home)

        workflows = Workflows.validate_workflows(from_root("/test/support/workflows/"), env)

        for workflow in workflows:
            workflow.register_to(env.workflow_home)

        assert(Workflows.list_workflows(env)['namespace1'][0].command == "workflow1")
        assert(list(Workflows.list_workflows(env, "namespace1").keys())[0] == "namespace1")
        assert(Workflows.list_workflows(env, "namespace1")["namespace1"][0].command == "workflow1")

        if os.path.exists(mason_home):
            shutil.rmtree(mason_home)

