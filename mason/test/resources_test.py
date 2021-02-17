from mason.configurations.config import Config
from mason.definitions import from_root
from mason.operators.operator import Operator
from mason.resources.base import get_all, get_resources, get_best_config, set_session_config
from mason.resources.malformed import MalformedResource
from mason.test.support import testing_base as base
from mason.util.logger import logger
import pytest
from mason.workflows.workflow import Workflow
from os import path, remove


class TestGetAll:
    
    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        logger.set_level("fatal")
        try:
            yield
        finally:
            cc = from_root("/test/support/configs/CURRENT_CONFIG")
            if path.exists(cc):
                remove(cc)
            
    @pytest.mark.skip(reason="Refactor resources/base first")
    def test_get_all(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        all1 = get_all(env, file=env.mason_home)
        all2 = get_all(env)
        assert(len(all1) == 28)
        assert(len(all2) == 28)
        
    def test_get_resources(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        operators = get_resources("operator", env)
        assert(len(operators) == 7)
        workflows = get_resources("workflow", env)
        assert(len(workflows) == 10)
        configs = get_resources("config", env)
        assert(len(configs) == 11)

        namespace1 = get_resources("operator", env, "namespace1")
        assert(len(namespace1) == 3)
        testing_namespace = get_resources("workflow", env, "testing_namespace")
        assert(len(testing_namespace) == 10)
        config5 = get_resources("config", env, "5")
        assert(len(config5) == 1)
        assert(isinstance(config5[0], Config))

        malformed_config = get_resources("config", env, "0")
        assert(len(malformed_config) == 1)
        assert(isinstance(malformed_config[0], MalformedResource))

        malformed_operator = get_resources("operators", env, "namespace1", "operator3")
        assert(len(malformed_operator) == 1)
        assert(isinstance(malformed_operator[0], MalformedResource))

        operator1 = get_resources("operators", env, "namespace1", "operator1")
        assert(len(operator1) == 1)
        assert(isinstance(operator1[0], Operator))

        workflow_cycle = get_resources("workflow", env, "testing_namespace", "workflow_cycle")
        assert(len(workflow_cycle) == 1)
        assert(isinstance(workflow_cycle[0], Workflow))
        
        best_config = get_best_config(env)
        assert(isinstance(best_config, Config))
        assert(best_config.id == '3')

        best_config = get_best_config(env, '5')
        assert(isinstance(best_config, Config))
        assert(best_config.id == '5')

        set_session_config(env, '4')
        best_config = get_best_config(env)
        assert(isinstance(best_config, Config))
        assert(best_config.id == '4')

