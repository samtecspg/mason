import os
import shutil

from definitions import from_root
from test.support import testing_base as base
import workflows as Workflows


class TestRegisterWorkflow:
    base.set_log_level("trace")
    mason_home = from_root("/.tmp/")
    operator_home = from_root("/.tmp/operators/")
    if os.path.exists(mason_home):
        shutil.rmtree(mason_home)

    env = base.get_env(workflow_home="/.tmp/workflows/")

    workflows, errors = Workflows.validate_workflows(from_root("/test/support/workflows"), True)


class TestGetWorkflow:
    pass

class TestListWorkflows:
    pass

class TestValidateWorkflows:
    pass
