from mason.engines import safe_interpolate_environment
from mason.engines.execution.execution_engine import ExecutionEngine
from mason.engines.metastore.metastore_engine import MetastoreEngine
from mason.engines.scheduler.scheduler_engine import SchedulerEngine
from mason.engines.storage.storage_engine import StorageEngine
from mason.util.environment import MasonEnvironment
from mason.test.support import testing_base as base
from mason.util.logger import logger
from mason.util.yaml import parse_yaml
from mason.definitions import from_root
from os import environ, unlink
import tempfile


class TestExecutionEngine:
    def before(self, config: str):
        base.set_log_level("error")
        config_home = from_root(config)
        env = MasonEnvironment(config_home=config_home)
        yaml_config_doc = parse_yaml(env.config_home)
        ee = ExecutionEngine(yaml_config_doc)
        return ee

    def test_configuration_exists(self):
        ee = self.before("/test/support/configs/test_config.yaml")
        assert(ee.client_name == "test2")
        assert(type(ee.client).__name__ == "InvalidExecutionClient")

    def test_configuration_dne(self):
        ee = self.before("/test/support/configs/test_partial_config.yaml")
        assert(ee.client_name == "")
        assert(type(ee.client).__name__ == "EmptyClient")

    def test_bad_config(self):
        ee = self.before("/test/support/configs/test_bad_config.yaml")
        assert(ee.client_name == "")
        assert(type(ee.client).__name__ == "EmptyClient")

class TestMetastoreEngine:
    def before(self, config: str):
        base.set_log_level("fatal")
        config_home = from_root(config)
        env = MasonEnvironment(config_home=config_home)
        yaml_config_doc = parse_yaml(env.config_home)
        me = MetastoreEngine(yaml_config_doc)
        return me

    def test_configuration_exists(self):
        me = self.before("/test/support/configs/test_config.yaml")
        assert(me.client_name == "test")
        assert(type(me.client).__name__ == "InvalidMetastoreClient")

    def test_configuration_dne(self):
        me = self.before("/test/support/configs/test_partial_config.yaml")
        assert(me.client_name == "")
        assert(type(me.client).__name__ == "EmptyClient")


class TestStorageEngine:
    def before(self, config: str):
        base.set_log_level("error")
        config_home = from_root(config)
        env = MasonEnvironment(config_home=config_home)
        yaml_config_doc = parse_yaml(env.config_home)
        me = StorageEngine(yaml_config_doc)
        return me

    def test_configuration_exists(self):
        me = self.before("/test/support/configs/test_config.yaml")
        assert(me.client_name == "test")
        assert(type(me.client).__name__ == "InvalidStorageClient")

    def test_configuration_dne(self):
        me = self.before("/test/support/configs/test_partial_config_2.yaml")
        assert(me.client_name == "")
        assert(type(me.client).__name__ == "EmptyClient")



class TestSchedulerEngine:
    def before(self, config: str):
        base.set_log_level("error")
        config_home = from_root(config)
        env = MasonEnvironment(config_home=config_home)
        yaml_config_doc = parse_yaml(env.config_home)
        me = SchedulerEngine(yaml_config_doc)
        return me

    def test_configuration_exists(self):
        me = self.before("/test/support/configs/test_config.yaml")
        assert(me.client_name == "test2")
        assert(type(me.client).__name__ == "InvalidSchedulerClient")

    def test_configuration_dne(self):
        me = self.before("/test/support/configs/test_partial_config.yaml")
        assert(me.client_name == "")
        assert(type(me.client).__name__ == "EmptyClient")

class TestEnvironmentInterpolation:
    def test_interpolation(self):
        logger.set_level("fatal")
        # valid interpolation
        environ["AWS_PROFILE"] = "default"
        environ["AWS_SECRET_ACCESS_KEY"] = "test-key"
        environ["SENSITIVE"] = "test-region"
        
        test_config = {"test": "{{AWS_REGION}}", "test2": "{{AWS_SECRET_ACCESS_KEY}}", "other": "{{SENSITIVE}}"}
        expect = {"test": "test-region", "test2": "test-key", "other": None}

        with tempfile.NamedTemporaryFile(delete=False, mode = "w") as tmp:
            tmp.write("""
            [default]
            aws_region = test-region 
            """)

        d = safe_interpolate_environment(test_config, tmp.name)
        assert(d == expect)












