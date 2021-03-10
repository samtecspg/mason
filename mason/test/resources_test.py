from mason.configurations.config import Config
from mason.definitions import from_root
from mason.operators.operator import Operator
from mason.resources.base import Resources
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
            
    def test_get_all(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        resources = Resources(env)
        all1 = resources.get_all()
        all2 = resources.get_all(env.mason_home)
        assert(len(all1) == 28)
        assert(len(all2) == 28)
        
    def test_get_resources(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        resources = Resources(env)
        operators = resources.get_operators()
        assert(len(operators) == 6)
        workflows = resources.get_workflows() 
        assert(len(workflows) == 10)
        configs = resources.get_configs()
        assert(len(configs) == 4)
        bad = resources.get_bad()
        assert(len(bad) == 8)

        namespace1 = resources.get_operators("namespace1")
        assert(len(namespace1) == 2)
        testing_namespace = resources.get_workflows("testing_namespace")
        assert(len(testing_namespace) == 10)
        config5 = resources.get_configs("5")
        assert(len(config5) == 1)
        assert(isinstance(config5[0], Config))

        malformed_config = resources.get_config("0")
        assert(isinstance(malformed_config, MalformedResource))

        malformed_operator = resources.get_operator("namespace1", "operator3")
        assert(isinstance(malformed_operator, MalformedResource))

        operator1 = resources.get_operator("namespace1", "operator1")
        assert(isinstance(operator1, Operator))

        workflow_cycle = resources.get_workflow("testing_namespace", "workflow_cycle")
        assert(isinstance(workflow_cycle, Workflow))

        best_config = resources.get_best_config()
        assert(isinstance(best_config, Config))
        assert(best_config.id == '3')

        best_config = resources.get_best_config('5')
        assert(isinstance(best_config, Config))
        assert(best_config.id == '5')
        
        resources.set_session_config('4')
        best_config = resources.get_best_config()
        assert(isinstance(best_config, Config))
        assert(best_config.id == '4')
